// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.


#pragma once
#include <sstream>
#include <string>
#include <boost/algorithm/string.hpp>
#include <boost/range/algorithm/replace_if.hpp>
#include "boost/thread/mutex.hpp"
#include "common/timer.h"

namespace baidu {
namespace galaxy {
class EventLog {
public:
    explicit EventLog(const std::string& module) :
        module_(module){
        ss_ << EventLog::EVENT_HEADER << " ";
        Append("__module__", module);
    }
    ~EventLog() {}

    template<typename T>
    EventLog& Append(const std::string& key, const T& value) {
        boost::mutex::scoped_lock lock(mutex_);

        if (ss_.str().size() > 0) {
            ss_ << " \1 ";
        }

        ss_ << key << "=" << value;
        return *this;
    }

    EventLog& Append(const std::string& key, const std::string& value) {
        boost::mutex::scoped_lock lock(mutex_);

        if (ss_.str().size() > 0) {
            ss_ << " \1 ";
        }

        ss_ << key << "=" << boost::replace_all_copy(value, "\n", "\2");
        return *this;
    }

    EventLog& AppendTime(const std::string& key) {
        boost::mutex::scoped_lock lock(mutex_);

        if (ss_.str().size() > 0) {
            ss_ << " \1 ";
        }

        ss_ << key << "=" << baidu::common::timer::get_micros();
        return *this;
    }


    EventLog& Reset() {
        boost::mutex::scoped_lock lock(mutex_);
        ss_.str("");
        ss_ << EventLog::EVENT_HEADER << " ";
        ss_ << " \1 ";
        ss_ << "__module__=" << module_;
        return *this;
    }

    template<typename T>
    EventLog& operator<<(const T& value) {
        boost::mutex::scoped_lock lock(mutex_);
        ss_ << value;
        return *this;
    }

    const std::string ToString() {
        boost::mutex::scoped_lock lock(mutex_);
        return ss_.str();
    }

private:
    std::stringstream ss_;
    boost::mutex mutex_;
    const static std::string EVENT_HEADER;
    const std::string module_;
};
}
}
