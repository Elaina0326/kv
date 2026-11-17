#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

namespace raftkv
{

    class RpcClient
    {
    public:
        explicit RpcClient(int timeout_ms = 200) : timeout_ms_(timeout_ms) {}
        ~RpcClient();

        RpcClient(const RpcClient &) = delete;
        RpcClient &operator=(const RpcClient &) = delete;

        bool Call(const std::string &addr, uint8_t type, std::string_view payload, std::string *reply);

        void CloseAll();

    private:
        int Connect(const std::string &addr);
        void Drop(const std::string &addr, int fd);
        bool SendAll(int fd, const char *data, size_t len);
        bool RecvAll(int fd, char *data, size_t len);

        int timeout_ms_;
        std::mutex mu_;
        std::unordered_map<std::string, int> fds_;
    };

} 
