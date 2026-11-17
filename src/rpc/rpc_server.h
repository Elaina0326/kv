#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common/thread_pool.h"
#include "net/tcp_server.h"
#include "rpc/messages.h"

namespace raftkv
{
    class RpcServer
    {
    public:

        using Handler = std::function<std::string(const std::string &payload)>;

        RpcServer(EventLoop *loop, std::string ip, int port, ThreadPool *pool);

        void Register(MsgType type, Handler handler);

        void RegisterRaft(MsgType type, Handler handler);
        void SetRaftPool(ThreadPool *pool) { raft_pool_ = pool; }
        bool Start(std::string *err);
        void Stop();

        int port() const { return server_.port(); }

    private:
        void OnMessage(const std::shared_ptr<Conn> &conn, uint8_t type, std::string payload);
        ThreadPool *PickPool(uint8_t type) const;

        EventLoop *loop_;
        ThreadPool *pool_;
        ThreadPool *raft_pool_ = nullptr;
        std::set<uint8_t> raft_types_;
        TcpServer server_;
        std::unordered_map<uint8_t, Handler> handlers_;
    };

} 
