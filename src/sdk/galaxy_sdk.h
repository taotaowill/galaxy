// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once

#include <string>
#include <stdint.h>
#include <vector>
#include <set>

namespace baidu {
namespace galaxy {

class RpcClient;

namespace sdk {

struct User {
    std::string user;
    std::string token;
};
struct Quota {
    int64_t millicore;
    int64_t memory;
    int64_t disk;
    int64_t ssd;
    int64_t replica;
};
enum Authority {
    kAuthorityCreateContainer = 1,
    kAuthorityRemoveContainer = 2,
    kAuthorityUpdateContainer = 3,
    kAuthorityListContainer = 4,
    kAuthoritySubmitJob = 5,
    kAuthorityRemoveJob = 6,
    kAuthorityUpdateJob = 7,
    kAuthorityListJobs = 8,
};
enum AuthorityAction {
    kActionAdd = 1,
    kActionRemove = 2,
    kActionSet = 3,
    kActionClear = 4,
};
enum ContainerType {
    kNormalContainer = 1,
    kVolumContainer = 2,
};
struct Grant {
    std::string pool;
    AuthorityAction action;
    std::vector<Authority> authority;
};
struct Resource {
    int64_t total;
    int64_t assigned;
    int64_t used;
};
enum VolumMedium {
    kSsd= 1,
    kDisk= 2,
    kBfs= 3,
    kTmpfs= 4,
};

enum ResourceError {
    kResOk = 0,
    kNoCpu = 1,
    kNoMemory = 2,
    kNoMedium = 3,
    kNoDevice = 4,
    kNoPort = 5,
    kPortConflict = 6,
    kTagMismatch = 7,
    kNoMemoryForTmpfs = 8,
    kPoolMismatch = 9,
    kTooManyPods = 10,
    kNoVolumContainer = 11,
};

struct VolumResource {
    VolumMedium medium;
    Resource volum;
    std::string device_path;
};
struct Volum {
    std::string path;
    int64_t used_size;
    int64_t assigned_size;
    VolumMedium medium;
};

struct CpuRequired {
    int64_t milli_core;
    bool excess;
};
struct MemoryRequired {
    MemoryRequired() :
        size(1024),
        excess(false),
        use_galaxy_killer(false) {}

    int64_t size;
    bool excess;
    bool use_galaxy_killer;
};

struct TcpthrotRequired {
    int64_t recv_bps_quota;
    bool recv_bps_excess;
    int64_t send_bps_quota;
    bool send_bps_excess;
};
struct BlkioRequired {
    int32_t weight;
};
struct PortRequired {
    std::string port_name;
    std::string port;
    std::string real_port;
};
enum VolumType {
    kEmptyDir = 1,
    kHostDir = 2,
};
struct VolumRequired {
    int64_t size;
    VolumType type;
    VolumMedium medium;
    std::string source_path;
    std::string dest_path;
    bool readonly;
    bool exclusive;
    bool use_symlink;
    bool preserved;
};
enum UpdateJobOperate {
    kUpdateJobStart = 1,
    kUpdateJobContinue = 2,
    kUpdateJobRollback = 3,
    kUpdateJobPause = 4,
    kUpdateJobCancel = 5,
};
enum JobType {
    kJobMonitor = 0,
    kJobService = 100,
    kJobBatch = 200,
    kJobBestEffort = 300,
};
enum JobStatus {
    kJobPending = 1,
    kJobRunning = 2,
    kJobFinished = 3,
    kJobDestroying = 4,
    kJobUpdating = 5,
    kJobUpdatePaused = 6,
};
enum PodStatus {
    kPodPending = 1,
    kPodReady = 2,
    kPodDeploying = 3,
    kPodStarting = 4,
    kPodServing = 5,
    kPodFailed = 6,
    kPodFinished = 7,
    kPodRunning = 8,
    kPodStopping = 9,
    kPodTerminated = 10,
};
enum TaskStatus {
    kTaskPending = 1,
    kTaskDeploying = 2,
    kTaskStarting = 3,
    kTaskServing = 4,
    kTaskFailed = 5,
    kTaskFinished = 6,
};
struct Package {
    std::string source_path;
    std::string dest_path;
    std::string version;
};
struct ImagePackage {
    ImagePackage() :
        stop_timeout(30) {}

    Package package;
    std::string start_cmd;
    std::string stop_cmd;
    int32_t stop_timeout;
    std::string health_cmd;
};
struct DataPackage {
    std::vector<Package> packages;
    std::string reload_cmd;
};
struct Deploy {
    Deploy() : replica(1),
    step(1),
    interval(1),
    max_per_host(1),
    update_break_count(1),
    stop_timeout(30) {
    }

    uint32_t replica;
    uint32_t step;
    uint32_t interval;
    uint32_t max_per_host;
    std::string tag;
    std::vector<std::string> pools;
    uint32_t update_break_count;
    uint32_t stop_timeout;
};
struct Service {
    std::string service_name;
    std::string port_name;
    bool use_bns;
    std::string tag;
    std::string health_check_type;
    std::string health_check_script;
    std::string token;
};
struct TaskDescription {
    std::string id;
    CpuRequired cpu;
    MemoryRequired memory;
    TcpthrotRequired tcp_throt;
    BlkioRequired blkio;
    std::vector<PortRequired> ports;
    ImagePackage exe_package;
    DataPackage data_package;
    std::vector<Service> services;
};
struct PodDescription {
    VolumRequired workspace_volum;
    std::vector<VolumRequired> data_volums;
    std::vector<TaskDescription> tasks;
};
enum VolumViewType {
    kVolumViewTypeEmpty = 0,
    kVolumViewTypeInner = 1,
    kVolumViewTypeExtra = 2
};
struct JobDescription {
    std::string name;
    JobType type;
    std::string version;
    std::vector<std::string> volum_jobs;
    Deploy deploy;
    PodDescription pod;
    std::string run_user;
    bool v2_support;
    VolumViewType volum_view;
};
struct Cgroup {
    std::string id;
    CpuRequired cpu;
    MemoryRequired memory;
    TcpthrotRequired tcp_throt;
    BlkioRequired blkio;
    std::vector<PortRequired> ports;
};
struct ContainerDescription {
    uint32_t priority;
    std::string run_user;
    std::string version;
    VolumRequired workspace_volum;
    std::vector<VolumRequired> data_volums;
    std::string cmd_line;
    std::vector<Cgroup> cgroups;
    int32_t max_per_host;
    std::string tag;
    std::vector<std::string> pool_names;
    std::vector<std::string> volum_jobs; //dependent volum jobs' id
    ContainerType container_type;
};
enum ContainerStatus {
    kContainerPending = 1,
    kContainerAllocating = 2,
    kContainerReady = 3,
    kContainerFinish = 4,      // finish , when appworker exit with code 0
    kContainerError = 5,
    kContainerDestroying = 6,
    kContainerTerminated = 7,
};
enum ContainerGroupStatus {
    kContainerGroupNormal = 1,
    kContainerGroupTerminated = 2,
};
enum Status {
   kOk = 1,
   kError = 2,
   kTerminate = 3,
   kAddAgentFail = 4,
   kDeny = 5,
   kJobNotFound = 6,
   kCreateContainerGroupFail = 7,
   kRemoveContainerGroupFail = 8,
   kUpdateContainerGroupFail = 9,
   kRemoveAgentFail = 10,
   kCreateTagFail = 11,
   kAddAgentToPoolFail = 12,
   kAddUserFail = 13,
   kRemoveUserFail = 14,
   kGrantUserFail = 15,
   kAssignQuotaFail = 16,
   kRebuild = 17,
   kReload = 18,
   kStatusConflict = 19,
   kJobTerminateFail = 20,
   kSuspend = 21,
   kQuit = 22,
   kPodNotFound = 23,
   kUserNotMatch = 24,
   kManualRebuild = 25,
   kManualReload = 26,
   kManualTerminate = 27,
   kManualQuit = 28,
};
struct ErrorCode {
    Status status;
    std::string reason;
};
enum AgentStatus {
    kAgentUnknown = 0,
    kAgentAlive = 1,
    kAgentDead = 2,
    kAgentOffline = 3,
    kAgentFreezed = 4,
};

struct EnterSafeModeRequest {
    User user;
};
struct EnterSafeModeResponse {
    ErrorCode error_code;
};
struct LeaveSafeModeRequest {
    User user;
};
struct LeaveSafeModeResponse {
    ErrorCode error_code;
};
struct StatusRequest {
    User user;
};
struct PoolStatus {
    std::string name;
    uint32_t total_agents;
    uint32_t alive_agents;
};
struct StatusResponse {
    ErrorCode error_code;
    uint32_t alive_agents;
    uint32_t dead_agents;
    uint32_t total_agents;
    Resource cpu;
    Resource memory;
    std::vector<VolumResource> volum;
    uint32_t total_groups;
    uint32_t total_containers;
    std::vector<PoolStatus> pools;
    bool in_safe_mode;
};

struct AddAgentRequest {
    User user;
    std::string endpoint;
    std::string pool;
};
struct AddAgentResponse {
    ErrorCode error_code;
};
struct RemoveAgentRequest {
    User user;
    std::string endpoint;
};
struct RemoveAgentResponse {
    ErrorCode error_code;
};
struct OnlineAgentRequest {
    User user;
    std::string endpoint;
};
struct OnlineAgentResponse {
    ErrorCode error_code;
};
struct OfflineAgentRequest {
    User user;
    std::string endpoint;
};
struct OfflineAgentResponse {
    ErrorCode error_code;
};

struct ListAgentsRequest {
    User user;
};
struct AgentStatistics {
    std::string endpoint;
    AgentStatus status;
    std::string pool;
    std::vector<std::string> tags;
    Resource cpu;
    Resource memory;
    std::vector<VolumResource> volums;
    uint32_t total_containers;
};
struct ListAgentsResponse {
    ErrorCode error_code;
    std::vector<AgentStatistics> agents;
};
struct CreateTagRequest {
    User user;
    std::string tag;
    std::vector<std::string> endpoint;
};
struct CreateTagResponse {
    ErrorCode error_code;
};
struct ListTagsRequest {
    User user;
};
struct ListTagsResponse {
    ErrorCode error_code;
    std::vector<std::string> tags;
};
struct ListAgentsByTagRequest {
    User user;
    std::string tag;
};
struct ListAgentsByTagResponse {
    ErrorCode error_code;
    std::vector<AgentStatistics> agents;
};
struct GetTagsByAgentRequest {
    User user;
    std::string endpoint;
};
struct GetTagsByAgentResponse {
    ErrorCode error_code;
    std::vector<std::string> tags;
};
struct AddAgentToPoolRequest {
    User user;
    std::string endpoint;
    std::string pool;
};
struct AddAgentToPoolResponse {
    ErrorCode error_code;
};
struct RemoveAgentFromPoolRequest {
    User user;
    std::string endpoint;
};
struct RemoveAgentFromPoolResponse {
    ErrorCode error_code;
};
struct ListAgentsByPoolRequest {
    User user;
    std::string pool;
};
struct ListAgentsByPoolResponse {
    ErrorCode error_code;
    std::vector<AgentStatistics> agents;
};
struct GetPoolByAgentRequest {
    User user;
    std::string endpoint;
};
struct GetPoolByAgentResponse {
    ErrorCode error_code;
    std::string pool;
};
struct AddUserRequest {
    User admin;
    User user;
};
struct AddUserResponse {
    ErrorCode error_code;
};
struct RemoveUserRequest {
    User admin;
    User user;
};
struct RemoveUserResponse {
    ErrorCode error_code;
};
struct ListUsersRequest {
    User user;
};
struct ListUsersResponse {
    ErrorCode error_code;
    std::vector<std::string> user;
};
struct ShowUserRequest {
    User admin;
    User user;
};
struct ShowUserResponse {
    ErrorCode error_code;
    std::vector<Grant> grants;
    Quota quota;
    Quota assigned;
};
struct GrantUserRequest {
    User admin;
    User user;
    Grant grant;
};
struct GrantUserResponse {
    ErrorCode error_code;
};
struct AssignQuotaRequest {
    User admin;
    User user;
    Quota quota;
};
struct AssignQuotaResponse {
    ErrorCode error_code;
};
struct FreezeAgentRequest {
    User user;
    std::string endpoint;
};
struct FreezeAgentResponse {
    ErrorCode error_code;
};
struct ThawAgentRequest {
    User user;
    std::string endpoint;
};
struct ThawAgentResponse {
    ErrorCode error_code;
};
struct CreateContainerGroupRequest {
    User user;
    uint32_t replica;
    std::string name;
    ContainerDescription desc;
};
struct CreateContainerGroupResponse {
    ErrorCode error_code;
    std::string id;
};
struct RemoveContainerGroupRequest {
    User user;
    std::string id;
};
struct RemoveContainerGroupResponse {
    ErrorCode error_code;
};
struct UpdateContainerGroupRequest {
    User user;
    uint32_t replica;
    std::string id;
    uint32_t interval;
    ContainerDescription desc;
};
struct UpdateContainerGroupResponse {
    ErrorCode error_code;
};
struct ListContainerGroupsRequest {
    User user;
};
struct ContainerGroupStatistics {
    std::string id;
    uint32_t replica;
    uint32_t ready;
    uint32_t pending;
    uint32_t allocating;
    Resource cpu;
    Resource memory;
    std::vector<VolumResource> volums;
    int64_t submit_time;
    int64_t update_time;
    std::string user_name;
    uint32_t destroying;
    ContainerType container_type;
};
struct ListContainerGroupsResponse {
    ErrorCode error_code;
    std::vector<ContainerGroupStatistics> containers;
};
struct ShowContainerGroupRequest {
    User user;
    std::string id;
};
struct ContainerStatistics {
    std::string id;
    ContainerStatus status;
    std::string endpoint;
    Resource cpu;
    Resource memory;
    std::vector<VolumResource> volums;
    ResourceError last_res_err;
};
struct ShowContainerGroupResponse {
    ErrorCode error_code;
    ContainerDescription desc;
    std::vector<ContainerStatistics> containers;
};

struct ShowAgentRequest {
    User user;
    std::string endpoint;
};

struct ShowAgentResponse {
    ErrorCode error_code;
    std::vector<ContainerStatistics> containers;
};

struct SubmitJobRequest {
    User user;
    JobDescription job;
    std::string hostname;
};
struct SubmitJobResponse {
    ErrorCode error_code;
    std::string jobid;
};
struct UpdateJobRequest {
    User user;
    std::string jobid;
    std::string hostname;
    JobDescription job;
    UpdateJobOperate operate;
    uint32_t update_break_count;
};
struct UpdateJobResponse {
    ErrorCode error_code;
};
struct RemoveJobRequest {
    User user;
    std::string jobid;
    std::string hostname;
};
struct RemoveJobResponse {
    ErrorCode error_code;
};
struct ListJobsRequest {
    User user;
};

struct JobOverview {
    JobDescription desc;
    std::string jobid;
    JobStatus status;
    int32_t running_num;
    int32_t pending_num;
    int32_t deploying_num;
    int32_t death_num;
    int32_t fail_count;
    int64_t create_time;
    int64_t update_time;
    std::string user;
};

struct ListJobsResponse {
    ErrorCode error_code;
    std::vector<JobOverview> jobs;
};
struct ShowJobRequest {
    User user;
    std::string jobid;
};

struct ServiceInfo {
    std::string name;
    std::string port;
    std::string ip;
    Status status;
};

struct PodInfo {
    std::string podid;
    std::string jobid;
    std::string endpoint;
    PodStatus status;
    std::string version;
    int64_t start_time;
    int64_t update_time;
    int32_t fail_count;
    std::vector<ServiceInfo> services;
};
struct JobInfo {
    std::string jobid;
    JobDescription desc;
    JobDescription last_desc;
    std::vector<PodInfo> pods;
    JobStatus status;
    std::string version;
    int64_t create_time;
    int64_t update_time;
    std::string user;
};
struct ShowJobResponse {
    ErrorCode error_code;
    JobInfo job;
};
struct ExecuteCmdRequest {
    User user;
    std::string jobid;
    std::string cmd;
};
struct ExecuteCmdResponse {
    ErrorCode error_code;
};

enum ForceAction {
    kForceActionNull = 1,
    kForceActionRebuild = 2,
    kForceActionReload = 3,
    kForceActionTerminate = 4,
    kForceActionQuit = 5,
};

struct ManualOperateRequest {
    User user;
    std::string jobid;
    std::string podid;
    ForceAction action;
};
struct ManualOperateResponse {
    ErrorCode error_code;
};

struct StopJobRequest {
    User user;
    std::string hostname;
    std::string jobid;
};
struct StopJobResponse {
    ErrorCode error_code;
};

struct PreemptRequest {
    User user;
    std::string container_group_id;
    std::string endpoint;
};

struct PreemptResponse {
     ErrorCode error_code;
};

struct RecoverInstanceRequest {
    User user;
    std::string jobid;
    std::string podid;
};

struct RecoverInstanceResponse {
    ErrorCode error_code;
};

struct RemoveTagsFromAgentRequest {
    User user;
    std::string endpoint;
    std::set<std::string> tags;
};

struct RemoveTagsFromAgentResponse {
    ErrorCode error_code;
};

} //namespace sdk
} //namespace galaxy
} //namespace baidu
