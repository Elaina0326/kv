#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "common/log_entry.h"
#include "common/types.h"
#include "rpc/messages.h"
#include "rpc/rpc_client.h"
#include "storage/wal.h"

namespace raftkv
{

    struct PeerInfo
    {
        int id = 0;
        std::string addr; 
    };

    struct RaftOptions
    {
        int node_id = 1;
        std::vector<PeerInfo> peers; 
        std::string data_dir;
        int election_timeout_ms = 300;
        int heartbeat_interval_ms = 100;
        int rpc_timeout_ms = 150;
        int tick_interval_ms = 10;
        int max_append_entries = 64;

        bool sync_wal = false;
    };

    enum class Role : uint8_t
    {
        kFollower = 0,
        kCandidate = 1,
        kLeader = 2
    };
    const char *RoleName(Role role);

    struct RaftStatus
    {
        int node_id = 0;
        Role role = Role::kFollower;
        uint64_t term = 0;
        uint64_t commit_index = 0;
        uint64_t last_applied = 0;
        uint64_t last_log_index = 0;
        int leader_id = 0;
        size_t peer_count = 0;
    };

    class Raft
    {
    public:

        using ApplyCallback = std::function<bool(uint64_t index, const LogEntry &entry)>;

        Raft(RaftOptions opts, ApplyCallback apply_cb);
        ~Raft();

        Raft(const Raft &) = delete;
        Raft &operator=(const Raft &) = delete;

        bool Start(std::string *err);

        bool BeginShutdown();
        void Stop();

        
        std::string HandleRequestVote(const std::string &payload);
        std::string HandleAppendEntries(const std::string &payload);

       
        struct ProposeResult
        {
            ErrorCode code = ErrorCode::kOk;
            uint64_t index = 0;
            uint64_t term = 0;
            int leader_hint = 0;
        };
    
        ProposeResult Propose(const std::string &command);

        struct ReadResult
        {
            ErrorCode code = ErrorCode::kOk;
            uint64_t index = 0;
            int leader_hint = 0;
        };
    
        ReadResult ReadIndex(int timeout_ms);

        bool WaitApplied(uint64_t index, int timeout_ms);

  
        uint64_t TermAt(uint64_t index) const;

        RaftStatus GetStatus() const;
        int LeaderId() const;
        Role CurrentRole() const;

    
        bool IsFatal() const;

        uint64_t last_index() const;

    private:
        struct RpcTask
        {
            uint8_t type = 0;
            std::string payload;
            uint64_t round = 0; 
            uint64_t term = 0;  
        };

        struct PeerState
        {
            int id = 0;
            std::string addr;

            uint64_t next_index = 1;
            uint64_t match_index = 0;
            uint64_t ack_round = 0;
            bool in_flight = false;
            bool force_send = false;
            int64_t last_send_ms = 0;

            std::mutex qmu;
            std::condition_variable qcv;
            std::deque<RpcTask> queue;
            bool stop = false;
            std::unique_ptr<RpcClient> client;
            std::thread thread;
        };

        struct PendingRead
        {
            uint64_t read_index = 0;
            int64_t deadline_ms = 0;
        };

        void TickerLoop();
        void ApplyLoop();
        void PeerLoop(int peer_id);

        void TickLocked();
        void StartElectionLocked();
        void BecomeLeaderLocked();
        void BecomeFollowerLocked(uint64_t term);
        void ResetElectionDeadlineLocked();

        void SendAppendEntriesLocked(PeerState *peer, bool force);
        void MaybeSendLocked(PeerState *peer, int64_t now);
        void BroadcastLocked(bool force);

        void HandleRequestVoteReplyLocked(int peer_id, const RequestVoteReply &reply, const RpcTask &task);
        void HandleAppendEntriesReplyLocked(int peer_id, const AppendEntriesReply &reply,
                                            const RpcTask &task);

        void AdvanceCommitLocked();
        void FailPendingReadsLocked();
        void TryConfirmReadsLocked();

        void EnqueueLocked(PeerState *peer, RpcTask task);
        bool PersistMetaLocked(std::string *err);

        void MarkFatalLocked(const char *reason);
 
        bool ValidateOptions(std::string *err) const;

        uint64_t LastLogIndexLocked() const;
        uint64_t LastLogTermLocked() const;
        uint64_t TermAtLocked(uint64_t index) const;
        uint64_t LastIndexOfTermLocked(uint64_t term) const;
        LogEntry &EntryLocked(uint64_t index) { return log_[static_cast<size_t>(index)]; }

        int Majority() const { return static_cast<int>(cluster_size_ / 2) + 1; }

        RaftOptions opts_;
        ApplyCallback apply_cb_;

        mutable std::mutex mu_;
        std::condition_variable apply_cv_;
        std::condition_variable wait_cv_;  
        std::condition_variable read_cv_; 

        std::unique_ptr<Wal> wal_;
        std::vector<LogEntry> log_; 
        std::map<int, PeerState> peers_;
        int self_id_ = 1;
        size_t cluster_size_ = 1;

        Role role_ = Role::kFollower;
        uint64_t current_term_ = 0;
        uint64_t voted_for_ = 0;
        int leader_id_ = 0;
        uint64_t commit_index_ = 0;
        uint64_t last_applied_ = 0;
        int64_t election_deadline_ms_ = 0;
        int current_votes_ = 0;
        std::set<int> voters_;
        uint64_t broadcast_round_ = 0;
        uint64_t confirmed_round_ = 0; 
        std::map<uint64_t, PendingRead> pending_reads_;

        uint64_t leader_noop_index_ = 0;

        std::atomic<bool> running_{false};
        bool started_ = false;    
        bool stop_logged_ = false; 
        bool fatal_ = false;
        std::string fatal_reason_;
        std::thread ticker_thread_;
        std::thread apply_thread_;
    };

} 
