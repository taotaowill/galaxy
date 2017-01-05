// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "galaxy_memory_subsystem.h"
#include "agent/util/path_tree.h"
#include "protocol/galaxy.pb.h"
#include "protocol/agent.pb.h"
#include "util/input_stream_file.h"

#include <unistd.h>
#include <signal.h>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>
#include <iostream>
#include <fstream>
#include <gflags/gflags.h>
#include <glog/logging.h>

DECLARE_string(agent_ip);
DECLARE_string(agent_port);
DECLARE_string(agent_hostname);
DECLARE_int32(oom_check_interval);

namespace baidu {
namespace galaxy {
namespace cgroup {

GalaxyMemorySubsystem::GalaxyMemorySubsystem(): background_pool_(1) {
}

GalaxyMemorySubsystem::~GalaxyMemorySubsystem() {
    background_pool_.Stop(true);
}

std::string GalaxyMemorySubsystem::Name() {
    return "memory";
}

baidu::galaxy::util::ErrorCode GalaxyMemorySubsystem::Collect(boost::shared_ptr<baidu::galaxy::proto::CgroupMetrix> metrix) {
    assert(NULL != metrix.get());

    // 1.set memory usage(rss + cache)
    boost::filesystem::path usage_path(Path());
    usage_path.append("memory.usage_in_bytes");
    baidu::galaxy::file::InputStreamFile in(usage_path.string());
    if (!in.IsOpen()) {
        baidu::galaxy::util::ErrorCode ec = in.GetLastError();
        return ERRORCODE(-1, "open file(%s) failed: %s",
                usage_path.string().c_str(),
                ec.Message().c_str());
    }

    std::string data;
    baidu::galaxy::util::ErrorCode ec = in.ReadLine(data);
    if (ec.Code() != 0) {
        return ERRORCODE(-1, "read file(%s) failed: %s",
                usage_path.string().c_str(),
                ec.Message().c_str());
    }
    metrix->set_memory_used_in_byte(::atol(data.c_str()));

    // 2.set memory cache usage
    boost::filesystem::path stat_path(Path());
    stat_path.append("memory.stat");
    std::string line;
    std::ifstream stat_file(stat_path.string().c_str());
    if (stat_file.is_open()) {
        while (getline(stat_file, line)) {
            std::istringstream ss(line);
            std::string name;
            uint64_t value;
            ss >> name >> value;
            if (name == "cache") {
                metrix->set_memory_cache_in_byte(value);
                break;
            }
        }
        stat_file.close();
    } else {
        return ERRORCODE(-1, "open file(%s) failed: %s",
                stat_path.string().c_str(),
                std::strerror(errno));
    }

    return ERRORCODE_OK;
}

baidu::galaxy::util::ErrorCode GalaxyMemorySubsystem::Construct() {
    assert(!this->container_id_.empty());
    assert(NULL != this->cgroup_.get());
    std::string path = this->Path();
    boost::system::error_code ec;

    if (!boost::filesystem::exists(path, ec)
            && !baidu::galaxy::file::create_directories(path, ec)) {
        return ERRORCODE(-1, "failed in creating file %s: %s",
                path.c_str(),
                ec.message().c_str());
    }

    boost::filesystem::path memory_limit_path = path;
    memory_limit_path.append("memory.limit_in_bytes");
    baidu::galaxy::util::ErrorCode err;
    err = baidu::galaxy::cgroup::Attach(memory_limit_path.c_str(),
            -1, false);

    if (0 != err.Code()) {
        return ERRORCODE(-1,
                "attch memory.limit_in_bytes failed: %s",
                err.Message().c_str());
    }

    boost::filesystem::path kill_mode_path = path;
    kill_mode_path.append("memory.kill_mode");
    err = baidu::galaxy::cgroup::Attach(kill_mode_path.c_str(),
            0L,
            false);

    if (0 != err.Code()) {
        return ERRORCODE(-1,
                "attch memory.kill_mode failed: %s",
                err.Message().c_str());
    }

    background_pool_.DelayTask(
        FLAGS_oom_check_interval,
        boost::bind(&GalaxyMemorySubsystem::OomCheckRoutine, this)
    );

    return ERRORCODE_OK;
}

boost::shared_ptr<Subsystem> GalaxyMemorySubsystem::Clone() {
    boost::shared_ptr<Subsystem> ret(new GalaxyMemorySubsystem());
    return ret;
}

void GalaxyMemorySubsystem::OomKill(int64_t usage, int64_t cache) {
    boost::filesystem::path cgroup_procs_path = this->Path();
    cgroup_procs_path.append("cgroup.procs");
    baidu::galaxy::file::InputStreamFile in(cgroup_procs_path.string());
    if (!in.IsOpen()) {
        LOG(WARNING)
            << "open cgroup.procs failed"
            << ", " << cgroup_procs_path.string()
            << ", " << in.GetLastError().Message();
        return;
    }

    std::string data;
    baidu::galaxy::util::ErrorCode ec = in.ReadLine(data);
    if (ec.Code() != 0) {
        LOG(WARNING)
            << "read cgroup.procs failed"
            << ", " << cgroup_procs_path.string()
            << ", " << ec.Message();
        return;
    }

    pid_t pid = ::atoi(data.c_str());
    pid_t pgid = getpgid(pid);
    if (pid == 0 || pgid == 0) {
        return;
    }

    killpg(pgid, SIGKILL);
    std::string warning_str = "galaxy oom killer killed pid: "\
        + boost::lexical_cast<std::string>(pid)\
        + ", pgid: " + boost::lexical_cast<std::string>(pgid)
        + ", usage: " + boost::lexical_cast<std::string>(usage)
        + ", cache:" + boost::lexical_cast<std::string>(cache);
    // ::baidu::galaxy::EventLog ev("container");
    // LOG(WARNING) << warning_str;
    // LOG(ERROR) << ev
    //     .AppendTime("time").Append("container-id", container_id_)
    //     .Append("endpoint", FLAGS_agent_ip + ":" + FLAGS_agent_port)
    //     .Append("hostname", FLAGS_agent_hostname)
    //     .Append("action", "oom").Append("status", "kOk")
    //     .Append("detail", warning_str)
    //     .ToString();

    return;
}

void GalaxyMemorySubsystem::OomCheckRoutine() {
    boost::shared_ptr<baidu::galaxy::proto::CgroupMetrix> metrix(new baidu::galaxy::proto::CgroupMetrix());
    Collect(metrix);
    VLOG(10)
        << "oom check routine"
        << ", container: " << container_id_
        << ", usage: "<< metrix->memory_used_in_byte()
        << ", cache: " << metrix->memory_cache_in_byte();

    uint64_t mem = metrix->memory_used_in_byte() - metrix->memory_cache_in_byte();
    if (mem > (uint64_t)cgroup_->memory().size()) {
        LOG(WARNING)
            << "cgroup memory oom"
            << ", container_id: " << container_id_
            << ", limit: " << cgroup_->memory().size()
            << ", usage: " << mem;
        OomKill(metrix->memory_used_in_byte(), metrix->memory_cache_in_byte());
    }

    background_pool_.DelayTask(
        FLAGS_oom_check_interval,
        boost::bind(&GalaxyMemorySubsystem::OomCheckRoutine, this)
    );

    return;
}

} //namespace cgroup
} //namespace galaxy
} //namespace baidu
