#include "rpc/rpc_client.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "common/codec.h"
#include "common/log.h"
#include "common/util.h"

namespace raftkv
{

    RpcClient::~RpcClient() { CloseAll(); }

    void RpcClient::CloseAll()
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto &kv : fds_)
        {
            if (kv.second >= 0)
                ::close(kv.second);
        }
        fds_.clear();
    }

    int RpcClient::Connect(const std::string &addr)
    {
        std::string host;
        int port = 0;
        if (!SplitHostPort(addr, &host, &port))
            return -1;

        int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd < 0)
            return -1;

        sockaddr_in sin{};
        sin.sin_family = AF_INET;
        sin.sin_port = htons(static_cast<uint16_t>(port));
        if (::inet_pton(AF_INET, host.c_str(), &sin.sin_addr) != 1)
        {
            ::close(fd);
            return -1;
        }

        
        SetNonBlocking(fd);
        int ret = ::connect(fd, reinterpret_cast<sockaddr *>(&sin), sizeof(sin));
        if (ret != 0 && errno != EINPROGRESS)
        {
            ::close(fd);
            return -1;
        }
        if (ret != 0)
        {
            pollfd pfd{fd, POLLOUT, 0};
            int n = ::poll(&pfd, 1, timeout_ms_);
            if (n <= 0)
            {
                ::close(fd);
                return -1;
            }
            int err = 0;
            socklen_t len = sizeof(err);
            if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0 || err != 0)
            {
                ::close(fd);
                return -1;
            }
        }

      
        int flags = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
        struct timeval tv{};
        tv.tv_sec = timeout_ms_ / 1000;
        tv.tv_usec = (timeout_ms_ % 1000) * 1000;
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        SetTcpNoDelay(fd);
        return fd;
    }

    void RpcClient::Drop(const std::string &addr, int fd)
    {
        auto it = fds_.find(addr);
        if (it != fds_.end() && it->second == fd)
            fds_.erase(it);
        ::close(fd);
    }

    bool RpcClient::SendAll(int fd, const char *data, size_t len)
    {
        size_t sent = 0;
        while (sent < len)
        {
            ssize_t n = ::send(fd, data + sent, len - sent, MSG_NOSIGNAL);
            if (n > 0)
            {
                sent += static_cast<size_t>(n);
                continue;
            }
            if (n < 0 && errno == EINTR)
                continue;
            return false;
        }
        return true;
    }

    bool RpcClient::RecvAll(int fd, char *data, size_t len)
    {
        size_t got = 0;
        while (got < len)
        {
            ssize_t n = ::recv(fd, data + got, len - got, 0);
            if (n > 0)
            {
                got += static_cast<size_t>(n);
                continue;
            }
            if (n < 0 && errno == EINTR)
                continue;
            return false;
        }
        return true;
    }

    bool RpcClient::Call(const std::string &addr, uint8_t type, std::string_view payload,
                         std::string *reply)
    {
        std::lock_guard<std::mutex> lock(mu_);
        int fd = -1;
        auto it = fds_.find(addr);
        if (it != fds_.end())
        {
            fd = it->second;
        }
        else
        {
            fd = Connect(addr);
            if (fd < 0)
                return false;
            fds_[addr] = fd;
        }

        std::string frame = EncodeFrame(type, payload);
        if (!SendAll(fd, frame.data(), frame.size()))
        {
            Drop(addr, fd);
            return false;
        }

 
        unsigned char header[4];
        if (!RecvAll(fd, reinterpret_cast<char *>(header), 4))
        {
            Drop(addr, fd);
            return false;
        }
        uint32_t len = (static_cast<uint32_t>(header[0]) << 24) |
                       (static_cast<uint32_t>(header[1]) << 16) |
                       (static_cast<uint32_t>(header[2]) << 8) | static_cast<uint32_t>(header[3]);
        if (len < 1 || len > kMaxFrameSize)
        {
            Drop(addr, fd);
            return false;
        }
        std::string body(len, '\0');
        if (!RecvAll(fd, body.data(), len))
        {
            Drop(addr, fd);
            return false;
        }
  
        reply->assign(body.data() + 1, len - 1);
        return true;
    }

} 
