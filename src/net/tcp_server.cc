#include "net/tcp_server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cassert>
#include <cstring>

#include "common/log.h"
#include "common/util.h"

namespace raftkv
{

    namespace
    {
        constexpr size_t kReadChunk = 64 * 1024;
        constexpr size_t kMaxReadPerEvent = 2 * 1024 * 1024; 
        constexpr size_t kMaxOutputBuffer = 4 * 1024 * 1024;
    }


    Conn::Conn(TcpServer *server, int fd, std::string peer)
        : server_(server), fd_(fd), peer_(std::move(peer)) {}

    Conn::~Conn()
    {
        if (fd_ >= 0)
            ::close(fd_);
    }

    void Conn::Start()
    {
        auto weak = weak_from_this();
        EventLoop *loop = server_->loop();
        loop->AddHandler(fd_, events_, [weak](uint32_t ev)
                         {
    if (auto conn = weak.lock()) conn->OnEvents(ev); });
        if (server_->conn_cb_)
            server_->conn_cb_(shared_from_this(), true);
    }

    void Conn::OnEvents(uint32_t events)
    {
        if (closed_)
            return;
        if (events & (EPOLLERR | EPOLLHUP))
        {
            Close();
            return;
        }
        if (events & (EPOLLIN | EPOLLRDHUP))
            HandleRead();
        if (closed_)
            return;
        if (events & EPOLLOUT)
            HandleWrite();
    }

    void Conn::HandleRead()
    {
        char buf[kReadChunk];
        size_t total = 0;
        bool peer_closed = false;
        while (total < kMaxReadPerEvent)
        {
            ssize_t n = ::read(fd_, buf, sizeof(buf));
            if (n > 0)
            {
                in_.Append(buf, static_cast<size_t>(n));
                total += static_cast<size_t>(n);
                if (static_cast<size_t>(n) < sizeof(buf))
                    break;
                continue;
            }
            if (n == 0)
            { 
                peer_closed = true;
                break;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            if (errno == EINTR)
                continue;
            LOG_WARN("conn %s read error: %s", peer_.c_str(), ErrnoString().c_str());
            Close();
            return;
        }

        while (!closed_)
        {
            std::string_view view = in_.Peek();
            if (view.size() < 4)
                break;
            const auto *p = reinterpret_cast<const unsigned char *>(view.data());
            uint32_t len = (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
                           (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
            if (len < 1 || len > kMaxFrameSize)
            {
                LOG_WARN("conn %s invalid frame len=%u, closing", peer_.c_str(), len);
                Close();
                return;
            }
            if (view.size() < 4 + len)
                break;
            uint8_t type = static_cast<uint8_t>(view[4]);
            std::string payload(view.substr(5, len - 1));
            in_.Consume(4 + len);
            if (server_->message_cb_)
                server_->message_cb_(shared_from_this(), type, std::move(payload));
        }
        
        if (peer_closed && !closed_)
            Close();
    }

    void Conn::HandleWrite()
    {
        if (out_.readable() > 0)
            FlushOutput();
        if (closing_ && out_.readable() == 0)
            DoClose();
    }

    void Conn::Send(uint8_t type, std::string_view payload)
    {
        assert(server_->loop()->IsInLoopThread());
        SendInLoop(type, payload);
    }

    void Conn::SendInLoop(uint8_t type, std::string_view payload)
    {
        if (closed_)
            return;
        std::string frame = EncodeFrame(type, payload);
        if (out_.readable() + frame.size() > kMaxOutputBuffer)
        {
            LOG_WARN("conn %s output buffer overflow (%zu + %zu bytes), closing", peer_.c_str(),
                     out_.readable(), frame.size());
            DoClose();
            return;
        }
        out_.Append(frame.data(), frame.size());
        FlushOutput();
        if (closed_)
            return;
        if (closing_ && out_.readable() == 0)
        {
            DoClose();
            return;
        }
        UpdateInterest();
    }

    void Conn::FlushOutput()
    {
        while (out_.readable() > 0)
        {
   
            ssize_t n = ::send(fd_, out_.data(), out_.readable(), MSG_NOSIGNAL);
            if (n > 0)
            {
                out_.Consume(static_cast<size_t>(n));
                continue;
            }
            if (n < 0 && errno == EINTR)
                continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
                break;
            LOG_WARN("conn %s write error: %s", peer_.c_str(), ErrnoString().c_str());
            DoClose();
            return;
        }
    }

    void Conn::UpdateInterest()
    {
        uint32_t want = EPOLLIN | EPOLLRDHUP;
        if (out_.readable() > 0)
            want |= EPOLLOUT;
        if (want != events_)
        {
            events_ = want;
            server_->loop()->UpdateEvents(fd_, want);
        }
    }

    void Conn::Close()
    {
        assert(server_->loop()->IsInLoopThread());
        if (closed_)
            return;
        if (out_.readable() > 0)
        { 
            closing_ = true;
            UpdateInterest();
            return;
        }
        DoClose();
    }

    void Conn::DoClose()
    {
        if (closed_)
            return;
        closed_ = true;
        auto self = shared_from_this(); 
        server_->loop()->RemoveHandler(fd_);
        server_->RemoveConn(fd_);
        if (server_->conn_cb_)
            server_->conn_cb_(self, false);
    }


    TcpServer::TcpServer(EventLoop *loop, std::string ip, int port)
        : loop_(loop), ip_(std::move(ip)), port_(port) {}

    TcpServer::~TcpServer() { Stop(); }

    void TcpServer::Stop()
    {

        std::vector<std::shared_ptr<Conn>> conns;
        conns.reserve(conns_.size());
        for (auto &kv : conns_)
            conns.push_back(kv.second);
        for (auto &conn : conns)
            conn->DoClose();
        conns_.clear();

        if (listen_fd_ >= 0)
        {
            loop_->RemoveHandler(listen_fd_);
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
    }

    bool TcpServer::Start(std::string *err)
    {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (listen_fd_ < 0)
        {
            *err = "socket failed: " + ErrnoString();
            return false;
        }
        int on = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port_));
        if (::inet_pton(AF_INET, ip_.c_str(), &addr.sin_addr) != 1)
        {
            *err = "invalid listen ip: " + ip_;
            return false;
        }
        if (::bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0)
        {
            *err = "bind " + ip_ + ":" + std::to_string(port_) + " failed: " + ErrnoString();
            return false;
        }
        if (::listen(listen_fd_, 1024) != 0)
        {
            *err = "listen failed: " + ErrnoString();
            return false;
        }

        sockaddr_in bound{};
        socklen_t len = sizeof(bound);
        if (::getsockname(listen_fd_, reinterpret_cast<sockaddr *>(&bound), &len) == 0)
        {
            port_ = ntohs(bound.sin_port);
        }

        if (!loop_->AddHandler(listen_fd_, EPOLLIN, [this](uint32_t)
                               { OnAccept(); }))
        {
            *err = "add listen handler failed: " + ErrnoString();
            return false;
        }
        LOG_INFO("tcp server listening on %s:%d", ip_.c_str(), port_);
        return true;
    }

    void TcpServer::OnAccept()
    {
        while (true)
        {
            sockaddr_in peer{};
            socklen_t len = sizeof(peer);
            int fd = ::accept4(listen_fd_, reinterpret_cast<sockaddr *>(&peer), &len,
                               SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (fd < 0)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    return;
                if (errno == EINTR)
                    continue;
                LOG_WARN("accept failed: %s", ErrnoString().c_str());
                return;
            }
            char ip_buf[INET_ADDRSTRLEN] = {0};
            ::inet_ntop(AF_INET, &peer.sin_addr, ip_buf, sizeof(ip_buf));
            std::string peer_str = std::string(ip_buf) + ":" + std::to_string(ntohs(peer.sin_port));
            SetTcpNoDelay(fd);
            SetKeepAlive(fd);

            if (conns_.size() >= 4096)
            {
                ::close(fd);
                continue;
            }
            auto conn = std::make_shared<Conn>(this, fd, peer_str);
            conns_[fd] = conn;
            conn->Start();
        }
    }

    void TcpServer::RemoveConn(int fd) { conns_.erase(fd); }

} 
