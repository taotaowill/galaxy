#include <gflags/gflags.h>

DEFINE_string(mount_templat, "", "mount templat");
DEFINE_string(mount_cgroups, "", "mount templat");

DEFINE_string(cgroup_root_path, "/cgroups", "cgroup root path");
DEFINE_string(galaxy_root_path, "", "galaxy work path");

DEFINE_string(nexus_root_path, "", "root path on nexus");
DEFINE_string(master_path, "", "master path");
DEFINE_string(nexus_servers, "", "servers of nexus cluster");

DEFINE_string(agent_ip, "", "agent ip");
DEFINE_string(agent_port, "1646", "agent listen port");
DEFINE_string(agent_hostname, "hostname", "agent hostname");
DEFINE_int32(keepalive_interval, 5000, "keep alive with RM");
DEFINE_int32(oom_check_interval, 5000, "galaxy oom killer check interval");

DEFINE_string(volum_resource, "", "volum resource, \
            format: filesystem:size_in_byte:mediu(DISK:SSD):mount_point, seperated by comma");
DEFINE_string(extra_volum_resource, "", "extra volum resource, \
            format: filesystem:size_in_byte:mediu(DISK:SSD):mount_point, seperated by comma");
DEFINE_int64(cpu_resource, 0L, "max millicores galaxy can use");
DEFINE_int64(memory_resource, 0L, "max memory(unit:byte) galaxy can use");

DEFINE_string(cmd_line, "", "just for debu");
DEFINE_int64(gc_delay_time, 43200, "");

DEFINE_int64(volum_collect_cycle, 18000, "");
DEFINE_int64(cgroup_collect_cycle, 5000, "");
DEFINE_string(v2_prefix, "/home/baidulinux/V2", "v2 prefix");

DEFINE_int32(assign_level, 2, "assign level: {0, 1, 2, 3}");
DEFINE_int32(check_assign_interval, 5000, "check assign interval");
DEFINE_int32(kill_timeout, 120, "kill appworker timeout");

