// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once

#include "subsystem.h"
#include <boost/shared_ptr.hpp>
#include <thread_pool.h>

namespace baidu {
namespace galaxy {
namespace cgroup {

class GalaxyMemorySubsystem : public Subsystem {
public:
    GalaxyMemorySubsystem();
    ~GalaxyMemorySubsystem();

    boost::shared_ptr<Subsystem> Clone();
    std::string Name();
    baidu::galaxy::util::ErrorCode Construct();
    baidu::galaxy::util::ErrorCode Collect(boost::shared_ptr<baidu::galaxy::proto::CgroupMetrix> metrix);

private:
    ThreadPool background_pool_;
    void OomCheckRoutine();
    void OomKill(int64_t usage, int64_t cache);
};

} //namespace cgroup
} //namespace galaxy
} //namespace baidu
