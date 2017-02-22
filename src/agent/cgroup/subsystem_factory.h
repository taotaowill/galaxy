// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once

#include "subsystem.h"

#include <boost/shared_ptr.hpp>
#include <boost/core/noncopyable.hpp>

#include <map>
#include <vector>

namespace baidu {
namespace galaxy {
namespace cgroup {

class SubsystemFactory : public boost::noncopyable {
public:
    static boost::shared_ptr<SubsystemFactory> GetInstance();
    void Setup();

    boost::shared_ptr<Subsystem> CreateSubsystem(const std::string& name, bool use_galaxy_killer = false);
    void GetSubsystems(std::vector<std::string>& subsystems);

private:
    SubsystemFactory();
    SubsystemFactory* Register(Subsystem* malloc_subsystem);
    static boost::shared_ptr<SubsystemFactory> s_instance_;
    std::map<const std::string, boost::shared_ptr<Subsystem> > cgroups_seed_;
    boost::shared_ptr<Subsystem> memory_;
    boost::shared_ptr<Subsystem> galaxy_memory_;
};

} //namespace cgroup
} //namespace galaxy
} //namespace baidu
