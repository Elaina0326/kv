#include "raft/raft.h"

#include <algorithm>
#include <chrono>
#include <cstdlib> 

#include "common/log.h"
#include "common/util.h"

namespace raftkv
{

    namespace
    {

        constexpr size_t kMaxAppendEntriesBytes = 4u * 1024 * 1024;
        constexpr size_t kAppendEntriesHeaderBytes = 40;
        constexpr size_t kAppendEntryOverheadBytes = 21;
    } 

    const char *RoleName(Role role)
    {
        switch (role)
        {
        case Role::kFollower:
            return "follower";
        case Role::kCandidate:
            return "candidate";
        case Role::kLeader:
            return "leader";
        }
        return "unknown";
    }

    Raft::Raft(RaftOptions opts, ApplyCallback apply_cb)
        : opts_(std::move(opts)), apply_cb_(std::move(apply_cb))
    {
        self_id_ = opts_.node_id;
        cluster_size_ = opts_.peers.size();
        log_.resize(1);
        log_[0].index = 0;
        log_[0].term = 0;
        log_[0].type = EntryType::kNoop;

        for (const auto &info : opts_.peers)
        {
            if (info.id == self_id_)
                continue; 
  
            PeerState &state = peers_.try_emplace(info.id).first->second;
            state.id = info.id;
            state.addr = info.addr;
            state.client = std::make_unique<RpcClient>(opts_.rpc_timeout_ms);
        }
        if (cluster_size_ == 0)
            cluster_size_ = 1;
        LOG_INFO("node %d: %zu peers, cluster size %zu, majority %d", self_id_, peers_.size(),
                 cluster_size_, Majority());
    }

    Raft::~Raft() { Stop(); }

    bool Raft::Start(std::string *err)
    {

        std::lock_guard<std::mutex> lk(mu_);
        if (running_)
            return true;

        if (!ValidateOptions(err))
            return false;
        if (!EnsureDir(opts_.data_dir))
        {
            *err = "create data dir failed: " + opts_.data_dir;
            return false;
        }
        wal_ = std::make_unique<Wal>();
        if (!wal_->Open(JoinPath(opts_.data_dir, "raft.wal"), opts_.sync_wal, err))
            return false;
        if (!wal_->Load(err))
            return false;

        log_.clear();
        LogEntry sentinel;
        sentinel.index = 0;
        sentinel.term = 0;
        sentinel.type = EntryType::kNoop;
        log_.push_back(std::move(sentinel));
        for (const auto &entry : wal_->entries())
            log_.push_back(entry);

        current_term_ = wal_->term();
        voted_for_ = wal_->voted_for();
 
        commit_index_ = std::min(wal_->commit_index(), LastLogIndexLocked());
        last_applied_ = 0;
        role_ = Role::kFollower;
        leader_id_ = 0;
        leader_noop_index_ = 0;
        ResetElectionDeadlineLocked();

        running_ = true;
        started_ = true;
        apply_thread_ = std::thread(&Raft::ApplyLoop, this);
        for (auto &kv : peers_)
        {
            PeerState *peer = &kv.second;
            peer->thread = std::thread(&Raft::PeerLoop, this, peer->id);
        }
        ticker_thread_ = std::thread(&Raft::TickerLoop, this);

        LOG_INFO("node %d: started, term=%llu log=%zu committed=%llu", self_id_,
                 static_cast<unsigned long long>(current_term_), log_.size() - 1,
                 static_cast<unsigned long long>(commit_index_));
        return true;
    }

    bool Raft::BeginShutdown()
    {
        if (!running_.exchange(false))
            return false;
        {
   
            std::lock_guard<std::mutex> lk(mu_);
        }
        apply_cv_.notify_all();
        wait_cv_.notify_all();
        read_cv_.notify_all();
        for (auto &kv : peers_)
        {
            {
                std::lock_guard<std::mutex> lk(kv.second.qmu);
                kv.second.stop = true;
            }
            kv.second.qcv.notify_all();
        }
        return true;
    }

    void Raft::Stop()
    {
        BeginShutdown(); 
        if (ticker_thread_.joinable())
            ticker_thread_.join();
        if (apply_thread_.joinable())
            apply_thread_.join();
        for (auto &kv : peers_)
        {
            if (kv.second.thread.joinable())
                kv.second.thread.join();
        }

        {
            std::lock_guard<std::mutex> lk(mu_);
            if (wal_)
                wal_->Close();
        }

        bool log_stop = false;
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (started_ && !stop_logged_)
            {
                stop_logged_ = true;
                log_stop = true;
            }
        }
        if (log_stop)
            LOG_INFO("node %d: stopped", self_id_);
    }



    void Raft::TickerLoop()
    {
        const auto interval = std::chrono::milliseconds(opts_.tick_interval_ms);
        while (running_)
        {
            std::this_thread::sleep_for(interval);
            if (!running_)
                break;
            std::lock_guard<std::mutex> lk(mu_);
            TickLocked();
        }
    }

    void Raft::TickLocked()
    {

        if (fatal_)
            return;
        const int64_t now = NowMs();
        if (role_ == Role::kLeader)
        {
            for (auto &kv : peers_)
                MaybeSendLocked(&kv.second, now);
 
            bool changed = false;
            for (auto it = pending_reads_.begin(); it != pending_reads_.end();)
            {
                if (it->second.deadline_ms <= now)
                {
                    it = pending_reads_.erase(it);
                    changed = true;
                }
                else
                {
                    ++it;
                }
            }
            if (changed)
                read_cv_.notify_all();
        }
        else if (now >= election_deadline_ms_)
        {
            StartElectionLocked();
        }
    }

    void Raft::ApplyLoop()
    {
        while (running_)
        {
            uint64_t index = 0;
            LogEntry entry;
            {
                std::unique_lock<std::mutex> lk(mu_);
                apply_cv_.wait(lk, [&]
                               { return !running_ || last_applied_ < commit_index_; });
                if (!running_)
                    break;
                if (last_applied_ >= commit_index_)
                    continue;
                index = last_applied_ + 1;
                entry = EntryLocked(index);
            }
            bool applied = true;
            if (entry.type == EntryType::kCommand && apply_cb_)
                applied = apply_cb_(index, entry);
            if (!applied)
            {

                LOG_ERROR("node %d: applying index %llu failed, entering unavailable state", self_id_,
                          static_cast<unsigned long long>(index));
                std::lock_guard<std::mutex> lk(mu_);
                MarkFatalLocked("apply failed");
                break;
            }
            {
                std::lock_guard<std::mutex> lk(mu_);
                if (last_applied_ < index)
                    last_applied_ = index;
            }
            wait_cv_.notify_all();
        }
    }

    void Raft::PeerLoop(int peer_id)
    {
        auto it = peers_.find(peer_id);
        if (it == peers_.end())
            return;
        PeerState *peer = &it->second;
        LOG_INFO("node %d: replication thread for peer %d (%s) started", self_id_, peer_id,
                 peer->addr.c_str());

        while (true)
        {
            RpcTask task;
            {
                std::unique_lock<std::mutex> lk(peer->qmu);
                peer->qcv.wait(lk, [&]
                               { return peer->stop || !peer->queue.empty(); });
                if (peer->stop && peer->queue.empty())
                    break;
                task = std::move(peer->queue.front());
                peer->queue.pop_front();
            }
            if (!running_)
                break;

            std::string reply_buf;
            const bool ok = peer->client->Call(peer->addr, task.type, task.payload, &reply_buf);

            std::lock_guard<std::mutex> lk(mu_);
            if (!running_)
                break;
            if (!ok)
            {
                
                auto pit = peers_.find(peer_id);
                if (pit != peers_.end())
                    pit->second.in_flight = false;
                continue;
            }
            if (task.type == static_cast<uint8_t>(MsgType::kRequestVote))
            {
                RequestVoteReply reply;
                if (DecodeRequestVoteReply(reply_buf, &reply))
                {
                    HandleRequestVoteReplyLocked(peer_id, reply, task);
                }
            }
            else if (task.type == static_cast<uint8_t>(MsgType::kAppendEntries))
            {
                AppendEntriesReply reply;
                if (DecodeAppendEntriesReply(reply_buf, &reply))
                {
                    HandleAppendEntriesReplyLocked(peer_id, reply, task);
                }
                else
                {
                    auto pit = peers_.find(peer_id);
                    if (pit != peers_.end())
                        pit->second.in_flight = false;
                }
            }
        }
    }


    void Raft::ResetElectionDeadlineLocked()
    {
        const int base = std::max(20, opts_.election_timeout_ms);
        
        election_deadline_ms_ = NowMs() + RandomInt(base, base * 2);
    }

    void Raft::StartElectionLocked()
    {
        ++current_term_;
        role_ = Role::kCandidate;
        leader_id_ = 0;
        voted_for_ = static_cast<uint64_t>(self_id_);
        current_votes_ = 1;
        voters_.clear();
        voters_.insert(self_id_);
        ResetElectionDeadlineLocked();
        FailPendingReadsLocked();

        std::string err;
        if (!PersistMetaLocked(&err))
        {

            LOG_ERROR("node %d: persist meta failed (%s), abandoning election for term %llu", self_id_,
                      err.c_str(), static_cast<unsigned long long>(current_term_));
            MarkFatalLocked("persist meta failed during election");
            role_ = Role::kFollower;
            return;
        }
        LOG_INFO("node %d: election timeout, starting election for term %llu", self_id_,
                 static_cast<unsigned long long>(current_term_));

        if (current_votes_ >= Majority())
        {
            BecomeLeaderLocked();
            return;
        }
        for (auto &kv : peers_)
        {
            RequestVoteArgs args;
            args.term = current_term_;
            args.candidate_id = self_id_;
            args.last_log_index = LastLogIndexLocked();
            args.last_log_term = LastLogTermLocked();
            EnqueueLocked(&kv.second, RpcTask{static_cast<uint8_t>(MsgType::kRequestVote),
                                              EncodeRequestVoteArgs(args), 0, current_term_});
        }
    }

    void Raft::BecomeLeaderLocked()
    {
        role_ = Role::kLeader;
        leader_id_ = self_id_;
        current_votes_ = 0;
        voters_.clear();
        FailPendingReadsLocked();
        broadcast_round_ = 0;
        confirmed_round_ = 0;
        leader_noop_index_ = 0;

        const uint64_t last = LastLogIndexLocked();
        for (auto &kv : peers_)
        {
            PeerState *peer = &kv.second;
            peer->next_index = last + 1;
            peer->match_index = 0;
            peer->ack_round = 0;
            peer->force_send = true;
        }
        LOG_INFO("node %d: became leader for term %llu (log last index %llu)", self_id_,
                 static_cast<unsigned long long>(current_term_), static_cast<unsigned long long>(last));

        LogEntry noop;
        noop.index = last + 1;
        noop.term = current_term_;
        noop.type = EntryType::kNoop;
        std::string err;
        if (!wal_->AppendEntry(noop, &err))
        {

            LOG_ERROR("node %d: append leader noop failed: %s", self_id_, err.c_str());
            MarkFatalLocked("append leader noop failed");
            role_ = Role::kFollower;
            leader_id_ = 0;
            leader_noop_index_ = 0;
            return;
        }
        log_.push_back(noop);
        leader_noop_index_ = noop.index;
        AdvanceCommitLocked(); 
        BroadcastLocked(true);
    }

    void Raft::BecomeFollowerLocked(uint64_t term)
    {
        const bool term_changed = term > current_term_;
        if (term_changed)
        {
            current_term_ = term;
            voted_for_ = 0;
            std::string err;
            if (!PersistMetaLocked(&err))
            {

                LOG_ERROR("node %d: persist meta failed: %s", self_id_, err.c_str());
                MarkFatalLocked("persist meta failed while stepping down");
            }
        }
        const bool was_leader = role_ == Role::kLeader;
        role_ = Role::kFollower;
        leader_id_ = 0;
        current_votes_ = 0;
        voters_.clear();
        leader_noop_index_ = 0;
        ResetElectionDeadlineLocked();
        FailPendingReadsLocked();
        if (was_leader)
        {
            LOG_INFO("node %d: stepped down to follower at term %llu", self_id_,
                     static_cast<unsigned long long>(current_term_));
        }
        if (term_changed)
        {
            LOG_INFO("node %d: term advanced to %llu", self_id_,
                     static_cast<unsigned long long>(current_term_));
        }
    }

    void Raft::FailPendingReadsLocked()
    {

        if (!pending_reads_.empty())
        {
            pending_reads_.clear();
            read_cv_.notify_all();
        }
        wait_cv_.notify_all();
    }

    void Raft::EnqueueLocked(PeerState *peer, RpcTask task)
    {
        {
            std::lock_guard<std::mutex> lk(peer->qmu);
            peer->queue.push_back(std::move(task));
        }
        peer->qcv.notify_one();
    }

    void Raft::MaybeSendLocked(PeerState *peer, int64_t now)
    {
        if (role_ != Role::kLeader || peer->in_flight)
            return;
        const int64_t interval = opts_.heartbeat_interval_ms;
        const bool due = now - peer->last_send_ms >= interval;
        if (!due && !peer->force_send)
            return;
        SendAppendEntriesLocked(peer, true);
    }

    void Raft::BroadcastLocked(bool force)
    {
        for (auto &kv : peers_)
        {
            if (force)
                kv.second.force_send = true;
            MaybeSendLocked(&kv.second, NowMs());
        }
    }

    void Raft::SendAppendEntriesLocked(PeerState *peer, bool force)
    {
        if (role_ != Role::kLeader)
            return;
        if (peer->in_flight)
            return;
        const int64_t now = NowMs();
        if (!force && now - peer->last_send_ms < opts_.heartbeat_interval_ms)
            return;

        const uint64_t last = LastLogIndexLocked();
        uint64_t next = std::max<uint64_t>(1, std::min(peer->next_index, last + 1));

        AppendEntriesArgs args;
        args.term = current_term_;
        args.leader_id = self_id_;
        args.leader_commit = commit_index_;
        args.prev_log_index = next - 1;
        args.prev_log_term = TermAtLocked(next - 1);
        if (last >= next)
        {

            const uint64_t max_entries = static_cast<uint64_t>(std::max(1, opts_.max_append_entries));
            size_t bytes = kAppendEntriesHeaderBytes;
            for (uint64_t i = next; i <= last && args.entries.size() < max_entries; ++i)
            {
                const LogEntry &entry = EntryLocked(i);
                const size_t cost = kAppendEntryOverheadBytes + entry.data.size();
         
                if (!args.entries.empty() && bytes + cost > kMaxAppendEntriesBytes)
                    break;
                bytes += cost;
                args.entries.push_back(entry);
            }
        }

        RpcTask task;
        task.type = static_cast<uint8_t>(MsgType::kAppendEntries);
        task.payload = EncodeAppendEntriesArgs(args);
        task.round = broadcast_round_;
        task.term = current_term_;
        EnqueueLocked(peer, std::move(task));

        peer->in_flight = true;
        peer->last_send_ms = now;
        peer->force_send = false;
    }

    void Raft::HandleAppendEntriesReplyLocked(int peer_id, const AppendEntriesReply &reply,
                                              const RpcTask &task)
    {
        auto it = peers_.find(peer_id);
        if (it == peers_.end())
            return;
        PeerState *peer = &it->second;
        peer->in_flight = false;

        if (reply.term > current_term_)
        {
            BecomeFollowerLocked(reply.term);
            return;
        }
        if (role_ != Role::kLeader || task.term != current_term_)
            return;

        const int64_t now = NowMs();
        if (!reply.success)
        {
            uint64_t next = peer->next_index > 1 ? peer->next_index - 1 : 1;
            if (reply.conflict_term > 0)
            {
                const uint64_t last_of_term = LastIndexOfTermLocked(reply.conflict_term);
                if (last_of_term > 0)
                {
                    next = std::min(next, last_of_term + 1);
                }
                else if (reply.conflict_index > 0)
                {
                    next = std::min(next, reply.conflict_index);
                }
            }
            else if (reply.conflict_index > 0)
            {
                next = std::min(next, reply.conflict_index);
            }
            peer->next_index = std::max<uint64_t>(1, next);
            peer->force_send = true;
            LOG_DEBUG("node %d: peer %d rejected AE (prev=%llu), backoff next_index=%llu", self_id_,
                      peer_id, static_cast<unsigned long long>(task.round),
                      static_cast<unsigned long long>(peer->next_index));
            MaybeSendLocked(peer, now);
            return;
        }

        if (reply.match_index > peer->match_index)
            peer->match_index = reply.match_index;
        peer->next_index = peer->match_index + 1;
        peer->ack_round = std::max(peer->ack_round, task.round);
        if (peer->match_index < LastLogIndexLocked())
            peer->force_send = true;

        AdvanceCommitLocked();
        TryConfirmReadsLocked();
        MaybeSendLocked(peer, now);
    }

    void Raft::HandleRequestVoteReplyLocked(int peer_id, const RequestVoteReply &reply,
                                            const RpcTask &task)
    {
        if (reply.term > current_term_)
        {
            BecomeFollowerLocked(reply.term);
            return;
        }
        if (role_ != Role::kCandidate || task.term != current_term_)
            return;
        if (!reply.granted)
            return;
        if (!voters_.insert(peer_id).second)
            return; 
        ++current_votes_;
        LOG_DEBUG("node %d: got vote from %d (%d/%d) in term %llu", self_id_, peer_id, current_votes_,
                  Majority(), static_cast<unsigned long long>(current_term_));
        if (current_votes_ >= Majority())
            BecomeLeaderLocked();
    }

    void Raft::AdvanceCommitLocked()
    {
        if (role_ != Role::kLeader)
            return;
        const uint64_t last = LastLogIndexLocked();
        if (last <= commit_index_)
            return;
        for (uint64_t n = last; n > commit_index_; --n)
        {

            if (TermAtLocked(n) != current_term_)
                continue;
            int count = 1; // 自己
            for (const auto &kv : peers_)
            {
                if (kv.second.match_index >= n)
                    ++count;
            }
            if (count >= Majority())
            {
                commit_index_ = n;
                std::string err;
                if (!wal_->AppendCommit(commit_index_, &err))
                {

                    LOG_ERROR("node %d: persist commit failed: %s", self_id_, err.c_str());
                    MarkFatalLocked("persist commit failed");
                }
                apply_cv_.notify_one();
                wait_cv_.notify_all();
                LOG_DEBUG("node %d: commit advanced to %llu", self_id_,
                          static_cast<unsigned long long>(commit_index_));
                break;
            }
        }
    }

    void Raft::TryConfirmReadsLocked()
    {
        if (role_ != Role::kLeader || pending_reads_.empty())
            return;
        bool advanced = false;
        for (auto it = pending_reads_.begin(); it != pending_reads_.end();)
        {
            int acked = 1;
            for (const auto &kv : peers_)
            {
                if (kv.second.ack_round >= it->first)
                    ++acked;
            }
            if (acked >= Majority())
            {
                confirmed_round_ = std::max(confirmed_round_, it->first);
                it = pending_reads_.erase(it);
                advanced = true;
            }
            else
            {
                ++it;
            }
        }
        if (advanced)
        {
            read_cv_.notify_all();
            wait_cv_.notify_all();
        }
    }

    

    std::string Raft::HandleRequestVote(const std::string &payload)
    {
        RequestVoteArgs args;
        RequestVoteReply reply;
        if (!DecodeRequestVoteArgs(payload, &args))
        {
            reply.term = 0;
            reply.granted = false;
            return EncodeRequestVoteReply(reply);
        }
        std::lock_guard<std::mutex> lk(mu_);
        reply.term = current_term_;

        if (args.term < current_term_)
        {
            reply.granted = false;
            return EncodeRequestVoteReply(reply);
        }
        if (args.term > current_term_)
            BecomeFollowerLocked(args.term);

        const uint64_t my_last_index = LastLogIndexLocked();
        const uint64_t my_last_term = LastLogTermLocked();
        const bool up_to_date = args.last_log_term > my_last_term ||
                                (args.last_log_term == my_last_term && args.last_log_index >= my_last_index);
        if (up_to_date && (voted_for_ == 0 || voted_for_ == static_cast<uint64_t>(args.candidate_id)))
        {
            const uint64_t prev_voted_for = voted_for_;
            voted_for_ = static_cast<uint64_t>(args.candidate_id);
            ResetElectionDeadlineLocked();
            std::string err;
            if (!PersistMetaLocked(&err))
            {

                LOG_ERROR("node %d: persist vote failed (%s), refusing to grant", self_id_, err.c_str());
                voted_for_ = prev_voted_for;
                MarkFatalLocked("persist vote failed");
                reply.granted = false;
                reply.term = current_term_;
                return EncodeRequestVoteReply(reply);
            }
            reply.granted = true;
            LOG_DEBUG("node %d: granted vote to %d at term %llu", self_id_, args.candidate_id,
                      static_cast<unsigned long long>(current_term_));
        }
        else
        {
            reply.granted = false;
        }
        reply.term = current_term_;
        return EncodeRequestVoteReply(reply);
    }

    std::string Raft::HandleAppendEntries(const std::string &payload)
    {
        AppendEntriesArgs args;
        AppendEntriesReply reply;
        if (!DecodeAppendEntriesArgs(payload, &args))
        {
            reply.term = 0;
            return EncodeAppendEntriesReply(reply);
        }
        std::lock_guard<std::mutex> lk(mu_);
        reply.term = current_term_;

        if (args.term < current_term_)
            return EncodeAppendEntriesReply(reply);

        if (args.term > current_term_)
            BecomeFollowerLocked(args.term);
        if (role_ != Role::kFollower)
        {
            role_ = Role::kFollower;
            current_votes_ = 0;
            voters_.clear();
            leader_noop_index_ = 0;
            FailPendingReadsLocked();
        }
        leader_id_ = args.leader_id;
        ResetElectionDeadlineLocked(); 
        reply.term = current_term_;

        const uint64_t my_last = LastLogIndexLocked();
        if (args.prev_log_index > my_last)
        {
            reply.success = false;
            reply.conflict_index = my_last + 1;
            reply.conflict_term = 0;
            return EncodeAppendEntriesReply(reply);
        }
        const uint64_t prev_term = TermAtLocked(args.prev_log_index);
        if (prev_term != args.prev_log_term)
        {
            uint64_t first = args.prev_log_index;
            while (first > 1 && TermAtLocked(first - 1) == prev_term)
                --first;
            reply.success = false;
            reply.conflict_term = prev_term;
            reply.conflict_index = first;
            return EncodeAppendEntriesReply(reply);
        }

        std::string err;
        uint64_t expected = args.prev_log_index + 1;
        for (const auto &entry : args.entries)
        {

            if (entry.index != expected)
            {
                LOG_ERROR("node %d: bad append frame: index %llu, expected %llu", self_id_,
                          static_cast<unsigned long long>(entry.index),
                          static_cast<unsigned long long>(expected));
                reply.success = false;
                reply.conflict_index = expected;
                reply.conflict_term = 0;
                return EncodeAppendEntriesReply(reply);
            }
            ++expected;
            if (entry.index <= LastLogIndexLocked())
            {
                if (TermAtLocked(entry.index) == entry.term)
                    continue;

                if (entry.index <= commit_index_)
                {
                    LOG_ERROR("node %d: refusing to truncate committed entry %llu (commit=%llu), aborting",
                              self_id_, static_cast<unsigned long long>(entry.index),
                              static_cast<unsigned long long>(commit_index_));
                    std::abort();
                }
                if (!wal_->AppendTruncate(entry.index, &err))
                {
                    LOG_ERROR("node %d: wal truncate failed: %s", self_id_, err.c_str());
                    MarkFatalLocked("wal truncate failed");

                    reply.success = false;
                    reply.conflict_index = LastLogIndexLocked() + 1;
                    reply.conflict_term = 0;
                    return EncodeAppendEntriesReply(reply);
                }
                log_.resize(static_cast<size_t>(entry.index));
                LOG_WARN("node %d: truncated log suffix from %llu", self_id_,
                         static_cast<unsigned long long>(entry.index));
            }
            if (!wal_->AppendEntry(entry, &err))
            {
                LOG_ERROR("node %d: wal append failed: %s", self_id_, err.c_str());
                MarkFatalLocked("wal append failed");
                reply.success = false;
                reply.conflict_index = LastLogIndexLocked() + 1;
                reply.conflict_term = 0;
                return EncodeAppendEntriesReply(reply);
            }
            log_.push_back(entry);
        }


        const uint64_t matched_end = args.prev_log_index + args.entries.size();
        if (args.leader_commit > commit_index_)
        {
            const uint64_t new_commit = std::min(args.leader_commit, matched_end);
            if (new_commit > commit_index_)
            {
                commit_index_ = new_commit;
                if (!wal_->AppendCommit(commit_index_, &err))
                {
                    LOG_ERROR("node %d: persist commit failed: %s", self_id_, err.c_str());
                    MarkFatalLocked("persist commit failed");
                }
                apply_cv_.notify_one();
                wait_cv_.notify_all();
            }
        }

        reply.success = true;
        reply.match_index = args.prev_log_index + args.entries.size();
        return EncodeAppendEntriesReply(reply);
    }



    Raft::ProposeResult Raft::Propose(const std::string &command)
    {
        std::lock_guard<std::mutex> lk(mu_);
        ProposeResult res;
        res.leader_hint = leader_id_;
        if (!running_)
        { 
            res.code = ErrorCode::kUnavailable;
            return res;
        }
        if (fatal_)
        { 
            res.code = ErrorCode::kUnavailable;
            return res;
        }
        if (role_ != Role::kLeader)
        {
            res.code = ErrorCode::kNotLeader;
            return res;
        }
        LogEntry entry;
        entry.index = LastLogIndexLocked() + 1;
        entry.term = current_term_;
        entry.type = EntryType::kCommand;
        entry.data = command;

        std::string err;
        if (!wal_->AppendEntry(entry, &err))
        {
            LOG_ERROR("node %d: wal append failed: %s", self_id_, err.c_str());

            MarkFatalLocked("wal append failed");
            res.code = ErrorCode::kInternal;
            res.leader_hint = self_id_;
            return res;
        }
        log_.push_back(entry);
        AdvanceCommitLocked();
        const int64_t now = NowMs();
        for (auto &kv : peers_)
        {
            kv.second.force_send = true;
            MaybeSendLocked(&kv.second, now);
        }
        res.code = ErrorCode::kOk;
        res.index = entry.index;
        res.term = entry.term; 
        res.leader_hint = self_id_;
        return res;
    }

    Raft::ReadResult Raft::ReadIndex(int timeout_ms)
    {
        const int64_t deadline = NowMs() + std::max(1, timeout_ms);
        std::unique_lock<std::mutex> lk(mu_);
        if (fatal_)
            return {ErrorCode::kUnavailable, 0, 0};
        if (role_ != Role::kLeader)
            return {ErrorCode::kNotLeader, 0, leader_id_};
        const uint64_t start_term = current_term_;

   
        while (commit_index_ < leader_noop_index_)
        {
            if (!running_ || fatal_)
                return {ErrorCode::kUnavailable, 0, 0};
            if (role_ != Role::kLeader || current_term_ != start_term)
            {
                return {ErrorCode::kNotLeader, 0, leader_id_};
            }
            const int64_t left = deadline - NowMs();
            if (left <= 0)
                return {ErrorCode::kTimeout, 0, self_id_};
    
            wait_cv_.wait_for(lk, std::chrono::milliseconds(left));
        }

        const uint64_t read_index = commit_index_;
        if (peers_.empty())
            return {ErrorCode::kOk, read_index, self_id_}; 
        const uint64_t round = ++broadcast_round_;
        pending_reads_[round] = PendingRead{read_index, deadline};
        for (auto &kv : peers_)
        {
            kv.second.force_send = true;
            SendAppendEntriesLocked(&kv.second, true);
        }

        while (confirmed_round_ < round)
        {
            if (!running_ || fatal_)
            {
                pending_reads_.erase(round);
                return {ErrorCode::kUnavailable, 0, 0}; 
            }
            if (role_ != Role::kLeader || current_term_ != start_term)
            {
                pending_reads_.erase(round);
                return {ErrorCode::kNotLeader, 0, leader_id_};
            }
            const int64_t left = deadline - NowMs();
            if (left <= 0)
            {
                pending_reads_.erase(round);
                return {ErrorCode::kTimeout, 0, self_id_};
            }

            read_cv_.wait_for(lk, std::chrono::milliseconds(left));
        }
        pending_reads_.erase(round);
        return {ErrorCode::kOk, read_index, self_id_};
    }

    bool Raft::WaitApplied(uint64_t index, int timeout_ms)
    {
        std::unique_lock<std::mutex> lk(mu_);

        wait_cv_.wait_for(lk, std::chrono::milliseconds(std::max(1, timeout_ms)),
                          [&]
                          { return last_applied_ >= index || !running_ || fatal_; });
        return last_applied_ >= index;
    }

    uint64_t Raft::TermAt(uint64_t index) const
    {
        std::lock_guard<std::mutex> lk(mu_);
        return TermAtLocked(index);
    }

    RaftStatus Raft::GetStatus() const
    {
        std::lock_guard<std::mutex> lk(mu_);
        RaftStatus status;
        status.node_id = self_id_;
        status.role = role_;
        status.term = current_term_;
        status.commit_index = commit_index_;
        status.last_applied = last_applied_;
        status.last_log_index = LastLogIndexLocked();
        status.leader_id = leader_id_;
        status.peer_count = cluster_size_;
        return status;
    }

    int Raft::LeaderId() const
    {
        std::lock_guard<std::mutex> lk(mu_);
        return leader_id_;
    }

    Role Raft::CurrentRole() const
    {
        std::lock_guard<std::mutex> lk(mu_);
        return role_;
    }

    uint64_t Raft::last_index() const
    {
        std::lock_guard<std::mutex> lk(mu_);
        return LastLogIndexLocked();
    }

 
    uint64_t Raft::LastLogIndexLocked() const
    {
        return log_.empty() ? 0 : log_.back().index;
    }

    uint64_t Raft::LastLogTermLocked() const
    {
        return log_.empty() ? 0 : log_.back().term;
    }

    uint64_t Raft::TermAtLocked(uint64_t index) const
    {
        if (index >= log_.size())
            return 0;
        return log_[static_cast<size_t>(index)].term;
    }

    uint64_t Raft::LastIndexOfTermLocked(uint64_t term) const
    {
        for (size_t i = log_.size(); i > 0; --i)
        {
            if (log_[i - 1].term == term)
                return log_[i - 1].index;
        }
        return 0;
    }

    bool Raft::PersistMetaLocked(std::string *err)
    {
        return wal_ != nullptr && wal_->AppendMeta(current_term_, voted_for_, err);
    }

    void Raft::MarkFatalLocked(const char *reason)
    {
        if (fatal_)
            return;
        fatal_ = true;
        fatal_reason_ = reason;

        role_ = Role::kFollower;
        leader_id_ = 0;
        leader_noop_index_ = 0;
        current_votes_ = 0;
        voters_.clear();
        FailPendingReadsLocked();
        LOG_ERROR("node %d: entering unavailable state: %s", self_id_, reason);
    
        apply_cv_.notify_all();
        wait_cv_.notify_all();
        read_cv_.notify_all();
    }

    bool Raft::IsFatal() const
    {
        std::lock_guard<std::mutex> lk(mu_);
        return fatal_;
    }

    bool Raft::ValidateOptions(std::string *err) const
    {
        if (opts_.node_id <= 0)
        {
            *err = "invalid node id " + std::to_string(opts_.node_id) + "（0 表示「没有 leader」，不能当节点 id）";
            return false;
        }
        if (opts_.peers.empty())
        {
            *err = "peer list is empty：单节点集群也要把自己写进 --peers";
            return false;
        }
        std::set<int> ids;
        std::set<std::string> addrs;
        bool self_found = false;
        for (const PeerInfo &peer : opts_.peers)
        {
            if (peer.id <= 0)
            {
                *err = "invalid peer id " + std::to_string(peer.id);
                return false;
            }
            std::string host;
            int port = 0;
            if (!SplitHostPort(peer.addr, &host, &port) || host.empty())
            {
                *err = "invalid peer address '" + peer.addr + "'（需要 host:port）";
                return false;
            }
            if (!ids.insert(peer.id).second)
            {
                *err = "duplicate peer id " + std::to_string(peer.id);
                return false;
            }
            if (!addrs.insert(peer.addr).second)
            {
                *err = "duplicate peer address " + peer.addr;
                return false;
            }
            if (peer.id == opts_.node_id)
                self_found = true;
        }
        if (!self_found)
        {

            *err = "node id " + std::to_string(opts_.node_id) +
                   " is not in --peers（cluster size 会算小，多数派随之变小 → 可能双主）";
            return false;
        }
        if (opts_.heartbeat_interval_ms <= 0 ||
            opts_.election_timeout_ms <= opts_.heartbeat_interval_ms)
        {
            *err = "election-timeout(" + std::to_string(opts_.election_timeout_ms) +
                   "ms) must be greater than heartbeat(" + std::to_string(opts_.heartbeat_interval_ms) +
                   "ms)";
            return false;
        }
        if (opts_.rpc_timeout_ms <= 0 || opts_.tick_interval_ms <= 0 || opts_.max_append_entries <= 0)
        {
            *err = "rpc-timeout / tick-interval / max-append-entries must be positive";
            return false;
        }
        return true;
    }

} 
