#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/thread_pool.h"
#include "net/event_loop.h"
#include "raft/raft.h"
#include "rpc/rpc_server.h"
#include "storage/kv_store.h"

namespace raftkv
{

    struct ServerOptions
    {
        std::string listen_ip = "0.0.0.0";
        int listen_port = 9201;
        int rpc_threads = 8;
        int raft_rpc_threads = 2;      
        int request_timeout_ms = 1500; 
        int sweep_interval_ms = 1000;
        RaftOptions raft;
    };

    class KVServer
    {
    public:
       
        static constexpr size_t kMaxKeyBytes = 4 * 1024;
        static constexpr size_t kMaxValueBytes = 1024 * 1024;    
        static constexpr size_t kMaxScanBytes = 2 * 1024 * 1024;
        static constexpr uint32_t kMaxScanLimit = 1000;
        static constexpr uint64_t kMaxTtlMs = 365ull * 24 * 3600 * 1000; 

        explicit KVServer(ServerOptions opts);
        ~KVServer();

        KVServer(const KVServer &) = delete;
        KVServer &operator=(const KVServer &) = delete;

        bool Start(std::string *err);
        void Run(); 
        void Stop();

    
        KVStore *store() { return &store_; }
        Raft *raft() { return raft_.get(); }

    private:
        void RegisterHandlers();
        std::string HandlePut(const std::string &payload);
        std::string HandleDelete(const std::string &payload);
        std::string HandleGet(const std::string &payload);
        std::string HandleScan(const std::string &payload);
        std::string HandleClientStatus(const std::string &payload);
 
        ErrorCode WaitFailureCode(int leader_hint, int *out_hint) const;

        ServerOptions opts_;
        KVStore store_;
        std::unique_ptr<EventLoop> loop_;
        std::unique_ptr<ThreadPool> pool_;
        std::unique_ptr<ThreadPool> raft_pool_;
        std::unique_ptr<RpcServer> rpc_;
        std::unique_ptr<Raft> raft_;
        std::thread sweep_thread_;
        std::atomic<bool> running_{false};
  
        std::mutex sweep_mu_;
        std::condition_variable sweep_cv_;
    };

} 
