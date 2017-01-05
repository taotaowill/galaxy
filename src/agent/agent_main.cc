// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "agent_impl.h"
#include "setting_utils.h"
#include "utils/event_log.h"

#include <sofa/pbrpc/pbrpc.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/wait.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <algorithm>
#include <vector>

DECLARE_string(agent_port);
DECLARE_string(agent_ip);
DECLARE_string(agent_hostname);

static volatile bool s_quit = false;
static void SignalIntHandler(int /*sig*/)
{
    s_quit = true;
}

void SigChldHandler(int /*sig*/)
{
    int status = 0;
    while (true) {
        pid_t pid = ::waitpid(-1, &status, WNOHANG);
        if (pid == -1 || pid == 0) {
            return;
        }
    }
}

int main(int argc, char* argv[])
{
    // set umask
    //umask(22);
    google::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    baidu::galaxy::SetupLog("agent");


    baidu::galaxy::AgentImpl* agent = new baidu::galaxy::AgentImpl();
    sofa::pbrpc::RpcServerOptions options;
    sofa::pbrpc::RpcServer rpc_server(options);

    if (!rpc_server.RegisterService(static_cast<baidu::galaxy::proto::Agent*>(agent))) {
        LOG(WARNING) << "failed to register agent service";
        exit(-1);
    }

    std::string endpoint = "0.0.0.0:" + FLAGS_agent_port;
    agent->Setup();

    if (!rpc_server.Start(endpoint)) {
        LOG(WARNING) << "failed to start server on " << endpoint;
        exit(-2);
    }

    baidu::galaxy::EventLog ev("agent");
    LOG(ERROR) << ev.Append("hostname", FLAGS_agent_hostname)
        .Append("endpoint", FLAGS_agent_ip + ":" + FLAGS_agent_port)
        .AppendTime("time")
        .Append("action", "start").ToString();

    signal(SIGINT, SignalIntHandler);
    signal(SIGTERM, SignalIntHandler);
    signal(SIGCHLD, SigChldHandler);
    LOG(INFO) << "agent started.";

    while (!s_quit) {
        sleep(1);
    }
    ev.Reset();
    LOG(ERROR) << ev.Append("agent", FLAGS_agent_hostname)
        .AppendTime("time")
        .Append("action", "stop").ToString();
    _exit(0);

    return 0;
}


