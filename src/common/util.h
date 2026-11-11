#pragma once

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace raftkv
{


    inline int64_t NowMs()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    inline int64_t NowUs()
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    inline int64_t WallMs()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    inline uint64_t RandomU64()
    {

        static thread_local uint64_t x = 0x9E3779B97F4A7C15ULL ^ static_cast<uint64_t>(NowUs());
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        return x * 0x2545F4914F6CDD1DULL;
    }

    inline int RandomInt(int lo, int hi)
    { 
        if (hi <= lo)
            return lo;
        return lo + static_cast<int>(RandomU64() % static_cast<uint64_t>(hi - lo + 1));
    }

    inline bool SetNonBlocking(int fd)
    {
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags < 0)
            return false;
        return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
    }

    inline void SetTcpNoDelay(int fd)
    {
        int on = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
    }

    inline void SetKeepAlive(int fd)
    {
        int on = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof(on));
    }

    inline std::vector<std::string> SplitString(const std::string &s, char delim)
    {
        std::vector<std::string> out;
        std::string cur;
        for (char c : s)
        {
            if (c == delim)
            {
                out.push_back(cur);
                cur.clear();
            }
            else
            {
                cur.push_back(c);
            }
        }
        if (!cur.empty())
            out.push_back(cur);
        return out;
    }

    inline bool SplitHostPort(const std::string &addr, std::string *host, int *port)
    {
        auto pos = addr.rfind(':');
        if (pos == std::string::npos)
            return false;
        *host = addr.substr(0, pos);
        *port = std::atoi(addr.c_str() + pos + 1);
        return *port > 0 && *port < 65536;
    }


    inline bool EnsureDir(const std::string &path)
    {
        if (path.empty())
            return false;
        struct stat st{};
        if (::stat(path.c_str(), &st) == 0)
            return S_ISDIR(st.st_mode);
        // 自底向上找第一个已存在的祖先，再逐级 mkdir
        const size_t slash = path.find_last_of('/');
        if (slash != std::string::npos && slash > 0)
        {
            if (!EnsureDir(path.substr(0, slash)))
                return false;
        }
        if (::mkdir(path.c_str(), 0755) == 0)
            return true;
 
        return ::stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
    }

    inline std::string JoinPath(const std::string &a, const std::string &b)
    {
        if (a.empty())
            return b;
        if (a.back() == '/')
            return a + b;
        return a + "/" + b;
    }

    inline std::string ErrnoString() { return std::string(std::strerror(errno)); }

} 
