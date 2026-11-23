#include "server/kv_server.h"

#include <algorithm>
#include <chrono>

#include "common/log.h"
#include "common/util.h"

namespace raftkv
{

    KVServer::KVServer(ServerOptions opts) : opts_(std::move(opts)) {}

    KVServer::~KVServer() { Stop(); }

    bool KVServer::Start(std::string *err)
    {
        loop_ = std::make_unique<EventLoop>();
        if (!loop_->Init(err))
            return false;

        pool_ = std::make_unique<ThreadPool>(static_cast<size_t>(std::max(1, opts_.rpc_threads)));
        raft_pool_ =
            std::make_unique<ThreadPool>(static_cast<size_t>(std::max(1, opts_.raft_rpc_threads)));


        raft_ = std::make_unique<Raft>(opts_.raft, [this](uint64_t index, const LogEntry &entry)
                                       {
    if (entry.type != EntryType::kCommand) return true;  
    const ErrorCode code = store_.Apply(entry.data);
    if (code != ErrorCode::kOk) {

      LOG_ERROR("apply index=%llu failed: %s", static_cast<unsigned long long>(index),
                ErrorCodeName(code));
      return false;
    }
    return true; });
        if (!raft_->Start(err))
            return false;

        rpc_ = std::make_unique<RpcServer>(loop_.get(), opts_.listen_ip, opts_.listen_port, pool_.get());
        rpc_->SetRaftPool(raft_pool_.get());
        RegisterHandlers();
        if (!rpc_->Start(err))
        {
            raft_->Stop();
            return false;
        }

        running_ = true;
        sweep_thread_ = std::thread([this]
                                    {
    const auto interval = std::chrono::milliseconds(std::max(100, opts_.sweep_interval_ms));
    while (true) {

      {
        std::unique_lock<std::mutex> lock(sweep_mu_);
        sweep_cv_.wait_for(lock, interval, [this] { return !running_.load(); });
        if (!running_) break;
      }
      const size_t removed = store_.SweepExpired();
      if (removed > 0) LOG_DEBUG("swept %zu expired keys", removed);
    } });

        LOG_INFO("node %d listening on %s:%d (%d client threads + %d raft threads), data dir %s",
                 opts_.raft.node_id, opts_.listen_ip.c_str(), rpc_->port(), opts_.rpc_threads,
                 opts_.raft_rpc_threads, opts_.raft.data_dir.c_str());
        return true;
    }

    void KVServer::Run() { loop_->Loop(); }

    void KVServer::Stop()
    {
        if (!running_.exchange(false))
            return;

        {
            std::lock_guard<std::mutex> lock(sweep_mu_);
        }
        sweep_cv_.notify_all();
        if (sweep_thread_.joinable())
            sweep_thread_.join();

        if (loop_)
        {
            loop_->Quit();
            if (!loop_->WaitStopped(3000))
            {
                LOG_WARN("node %d: event loop did not stop in 3s", opts_.raft.node_id);
            }
        }
        if (rpc_)
            rpc_->Stop();

        if (raft_)
            raft_->BeginShutdown();

        if (pool_)
            pool_->Stop();
        if (raft_pool_)
            raft_pool_->Stop();
        if (raft_)
            raft_->Stop();
        LOG_INFO("node %d stopped", opts_.raft.node_id);
    }

    void KVServer::RegisterHandlers()
    {
        rpc_->RegisterRaft(MsgType::kRequestVote,
                           [this](const std::string &p)
                           { return raft_->HandleRequestVote(p); });
        rpc_->RegisterRaft(MsgType::kAppendEntries,
                           [this](const std::string &p)
                           { return raft_->HandleAppendEntries(p); });
        rpc_->Register(MsgType::kClientPut, [this](const std::string &p)
                       { return HandlePut(p); });
        rpc_->Register(MsgType::kClientDelete, [this](const std::string &p)
                       { return HandleDelete(p); });
        rpc_->Register(MsgType::kClientGet, [this](const std::string &p)
                       { return HandleGet(p); });
        rpc_->Register(MsgType::kClientScan, [this](const std::string &p)
                       { return HandleScan(p); });
        rpc_->Register(MsgType::kClientStatus,
                       [this](const std::string &p)
                       { return HandleClientStatus(p); });
    }

    std::string KVServer::HandlePut(const std::string &payload)
    {
        PutRequest req;
        if (!DecodePutRequest(payload, &req))
            return EncodeOkReply(ErrorCode::kBadRequest, 0);
 
        if (req.key.empty() || req.key.size() > kMaxKeyBytes || req.value.size() > kMaxValueBytes)
        {
            return EncodeOkReply(ErrorCode::kBadRequest, 0);
        }

        if (req.client_id == 0 || req.seq == 0)
            return EncodeOkReply(ErrorCode::kBadRequest, 0);
    
        if (req.ttl_ms > kMaxTtlMs)
            return EncodeOkReply(ErrorCode::kBadRequest, 0);

        Command cmd;
        cmd.op = Command::Op::kPut;
        cmd.client_id = req.client_id;
        cmd.seq = req.seq;
        cmd.key = req.key;
        cmd.value = std::move(req.value);
        cmd.expire_at_ms = req.ttl_ms > 0 ? static_cast<uint64_t>(WallMs()) + req.ttl_ms : 0;

        const Raft::ProposeResult res = raft_->Propose(EncodeCommand(cmd));
        if (res.code != ErrorCode::kOk)
            return EncodeOkReply(res.code, res.leader_hint);

    
        if (!raft_->WaitApplied(res.index, opts_.request_timeout_ms))
        {
            int hint = res.leader_hint;
            const ErrorCode code = WaitFailureCode(res.leader_hint, &hint);
            return EncodeOkReply(code, hint);
        }

        if (raft_->TermAt(res.index) != res.term)
        {
            LOG_WARN("node %d: proposal at index %llu (term %llu) was overwritten", opts_.raft.node_id,
                     static_cast<unsigned long long>(res.index),
                     static_cast<unsigned long long>(res.term));
            return EncodeOkReply(ErrorCode::kTimeout, res.leader_hint);
        }
        return EncodeOkReply(ErrorCode::kOk, res.leader_hint);
    }

    std::string KVServer::HandleDelete(const std::string &payload)
    {
        DeleteRequest req;
        if (!DecodeDeleteRequest(payload, &req))
            return EncodeOkReply(ErrorCode::kBadRequest, 0);
        if (req.key.empty() || req.key.size() > kMaxKeyBytes)
        {
            return EncodeOkReply(ErrorCode::kBadRequest, 0);
        }
        if (req.client_id == 0 || req.seq == 0)
            return EncodeOkReply(ErrorCode::kBadRequest, 0);
        Command cmd;
        cmd.op = Command::Op::kDelete;
        cmd.client_id = req.client_id;
        cmd.seq = req.seq;
        cmd.key = req.key;

        const Raft::ProposeResult res = raft_->Propose(EncodeCommand(cmd));
        if (res.code != ErrorCode::kOk)
            return EncodeOkReply(res.code, res.leader_hint);
        if (!raft_->WaitApplied(res.index, opts_.request_timeout_ms))
        {
            int hint = res.leader_hint;
            const ErrorCode code = WaitFailureCode(res.leader_hint, &hint);
            return EncodeOkReply(code, hint);
        }
        if (raft_->TermAt(res.index) != res.term)
        {
            return EncodeOkReply(ErrorCode::kTimeout, res.leader_hint);
        }
        return EncodeOkReply(ErrorCode::kOk, res.leader_hint);
    }

    std::string KVServer::HandleGet(const std::string &payload)
    {
        GetRequest req;
        if (!DecodeGetRequest(payload, &req))
            return EncodeValueReply(ErrorCode::kBadRequest, 0, "");

        const Raft::ReadResult read = raft_->ReadIndex(opts_.request_timeout_ms);
        if (read.code != ErrorCode::kOk)
            return EncodeValueReply(read.code, read.leader_hint, "");
        if (!raft_->WaitApplied(read.index, opts_.request_timeout_ms))
        {
            int hint = read.leader_hint;
            const ErrorCode code = WaitFailureCode(read.leader_hint, &hint);
            return EncodeValueReply(code, hint, "");
        }
        std::string value;
        const ErrorCode code = store_.Get(req.key, &value);
        if (code != ErrorCode::kOk)
            return EncodeValueReply(code, read.leader_hint, "");
        return EncodeValueReply(ErrorCode::kOk, read.leader_hint, value);
    }

    std::string KVServer::HandleScan(const std::string &payload)
    {
        ScanRequest req;
        if (!DecodeScanRequest(payload, &req))
        {
            return EncodeScanReply(ErrorCode::kBadRequest, 0, {});
        }
        const Raft::ReadResult read = raft_->ReadIndex(opts_.request_timeout_ms);
        if (read.code != ErrorCode::kOk)
            return EncodeScanReply(read.code, read.leader_hint, {});
        if (!raft_->WaitApplied(read.index, opts_.request_timeout_ms))
        {
            int hint = read.leader_hint;
            const ErrorCode code = WaitFailureCode(read.leader_hint, &hint);
            return EncodeScanReply(code, hint, {});
        }

        uint32_t limit = req.limit == 0 ? 100 : req.limit;
        if (limit > kMaxScanLimit)
            limit = kMaxScanLimit;
        std::vector<std::pair<std::string, std::string>> kvs;
        const ErrorCode code = store_.Scan(req.start, req.end, limit, &kvs, kMaxScanBytes);
        if (code != ErrorCode::kOk)
            return EncodeScanReply(code, read.leader_hint, {});
        return EncodeScanReply(ErrorCode::kOk, read.leader_hint, kvs);
    }

    ErrorCode KVServer::WaitFailureCode(int leader_hint, int *out_hint) const
    {
        if (raft_->IsFatal())
        {
  
            *out_hint = 0;
            return ErrorCode::kUnavailable;
        }
        if (raft_->CurrentRole() != Role::kLeader)
        {

            *out_hint = raft_->LeaderId();
            return ErrorCode::kNotLeader;
        }
        *out_hint = leader_hint;
        return ErrorCode::kTimeout; 
    }

    std::string KVServer::HandleClientStatus(const std::string &payload)
    {
        (void)payload;
        const RaftStatus raft_status = raft_->GetStatus();
        NodeStatus status;
        status.node_id = raft_status.node_id;
        status.role = static_cast<uint8_t>(raft_status.role);
        status.term = raft_status.term;
        status.commit_index = raft_status.commit_index;
        status.last_applied = raft_status.last_applied;
        status.last_log_index = raft_status.last_log_index;
        status.leader_id = raft_status.leader_id;
        status.key_count = store_.size();
        status.applied_commands = store_.applied_commands();
        return EncodeNodeStatusReply(status);
    }

} 
