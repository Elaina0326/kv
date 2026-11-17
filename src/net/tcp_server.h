#pragma once

#include <netinet/in.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common/codec.h"
#include "net/event_loop.h"

namespace raftkv
{

 
    class Buffer
    {
    public:
        size_t readable() const { return buf_.size() - read_pos_; }
        const char *data() const { return buf_.data() + read_pos_; }
        std::string_view Peek() const { return std::string_view(data(), readable()); }

        void Append(const char *d, size_t n)
        {
            buf_.append(d, n);
            CompactIfNeeded();
        }
        void Consume(size_t n)
        {
            read_pos_ += n;
            if (read_pos_ >= buf_.size())
            {
                buf_.clear();
                read_pos_ = 0;
            }
            CompactIfNeeded();
        }
        void Clear()
        {
            buf_.clear();
            read_pos_ = 0;
        }

    private:
    
        void CompactIfNeeded()
        {
            if (read_pos_ > 65536 && read_pos_ * 2 > buf_.size())
            {
                buf_.erase(0, read_pos_);
                read_pos_ = 0;
            }
        }

        std::string buf_;
        size_t read_pos_ = 0;
    };

    class TcpServer;

    class Conn : public std::enable_shared_from_this<Conn>
    {
    public:
        Conn(TcpServer *server, int fd, std::string peer);
        ~Conn();

        void Start();

        void Send(uint8_t type, std::string_view payload);
        void Close(); 
        int fd() const { return fd_; }
        const std::string &peer() const { return peer_; }
        bool closed() const { return closed_; } 

    private:
        friend class TcpServer;
        void OnEvents(uint32_t events);
        void HandleRead();
        void HandleWrite();
        void SendInLoop(uint8_t type, std::string_view payload);
        void FlushOutput();
        void UpdateInterest();
        void DoClose();

        TcpServer *server_;
        int fd_;
        std::string peer_;
        Buffer in_;
        Buffer out_;
        uint32_t events_ = EPOLLIN | EPOLLRDHUP;
        bool closed_ = false;
        bool closing_ = false; 
    };

    class TcpServer
    {
    public:
        using MessageCallback =
            std::function<void(const std::shared_ptr<Conn> &conn, uint8_t type, std::string payload)>;
        using ConnectionCallback = std::function<void(const std::shared_ptr<Conn> &conn, bool connected)>;

        TcpServer(EventLoop *loop, std::string ip, int port);
        ~TcpServer();

        TcpServer(const TcpServer &) = delete;
        TcpServer &operator=(const TcpServer &) = delete;

        void SetMessageCallback(MessageCallback cb) { message_cb_ = std::move(cb); }
        void SetConnectionCallback(ConnectionCallback cb) { conn_cb_ = std::move(cb); }

        bool Start(std::string *err);
        void Stop();

        EventLoop *loop() const { return loop_; }
        int port() const { return port_; }


    private:
        friend class Conn;
        void OnAccept();
        void RemoveConn(int fd);

        EventLoop *loop_;
        std::string ip_;
        int port_;
        int listen_fd_ = -1;
        std::unordered_map<int, std::shared_ptr<Conn>> conns_;
        MessageCallback message_cb_;
        ConnectionCallback conn_cb_;
    };

} 
