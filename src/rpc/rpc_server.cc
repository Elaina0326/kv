#include "rpc/rpc_server.h"

#include "common/log.h"

namespace raftkv
{

    RpcServer::RpcServer(EventLoop *loop, std::string ip, int port, ThreadPool *pool)
        : loop_(loop), pool_(pool), server_(loop, std::move(ip), port)
    {
        server_.SetMessageCallback(
            [this](const std::shared_ptr<Conn> &conn, uint8_t type, std::string payload)
            {
                OnMessage(conn, type, std::move(payload));
            });
    }

    void RpcServer::Register(MsgType type, Handler handler)
    {
        handlers_[static_cast<uint8_t>(type)] = std::move(handler);
    }

    void RpcServer::RegisterRaft(MsgType type, Handler handler)
    {
        raft_types_.insert(static_cast<uint8_t>(type));
        Register(type, std::move(handler));
    }

    ThreadPool *RpcServer::PickPool(uint8_t type) const
    {
        if (raft_pool_ != nullptr && raft_types_.count(type) != 0)
            return raft_pool_;
        return pool_;
    }

    bool RpcServer::Start(std::string *err) { return server_.Start(err); }

    void RpcServer::Stop() { server_.Stop(); }

    void RpcServer::OnMessage(const std::shared_ptr<Conn> &conn, uint8_t type, std::string payload)
    {
        auto it = handlers_.find(type);
        if (it == handlers_.end())
        {
            LOG_WARN("unknown message type=%u from %s", type, conn->peer().c_str());
            conn->Send(type, EncodeOkReply(ErrorCode::kBadRequest, 0));
            return;
        }
        Handler handler = it->second; 
        ThreadPool *pool = PickPool(type);
        pool->Submit([this, handler = std::move(handler), conn, type, payload = std::move(payload)]()
                     {
    std::string reply;
    try {
      reply = handler(payload);
    } catch (const std::exception& e) {
      LOG_ERROR("handler type=%u threw: %s", type, e.what());
      reply = EncodeOkReply(ErrorCode::kInternal, 0);
    }
    loop_->RunInLoop([conn, type, reply = std::move(reply)]() mutable {
      conn->Send(type, reply);
    }); });
    }

} 
