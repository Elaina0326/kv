#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "common/types.h"
#include "rpc/messages.h"
#include "rpc/rpc_client.h"

namespace raftkv
{

    class KVClient
    {
    public:
    
        explicit KVClient(std::vector<std::string> addrs, int timeout_ms = 2000);

        KVClient(const KVClient &) = delete;
        KVClient &operator=(const KVClient &) = delete;

        ErrorCode Put(const std::string &key, const std::string &value, uint64_t ttl_ms = 0);
        ErrorCode Get(const std::string &key, std::string *value);
        ErrorCode Delete(const std::string &key);
        ErrorCode Scan(const std::string &start, const std::string &end, uint32_t limit,
                       std::vector<std::pair<std::string, std::string>> *out);

  
        ErrorCode Status(NodeStatus *status, int node_index);

        size_t node_count() const { return addrs_.size(); }
        uint64_t client_id() const { return client_id_; }
        int current_node() const { return current_; }

    private:

        bool Invoke(uint8_t type, const std::string &payload, std::string *reply, ErrorCode *code);

 
        ErrorCode InvokeWithRetry(uint8_t type, const std::string &payload, std::string *reply);

        std::vector<std::string> addrs_;
        RpcClient rpc_;
        int timeout_ms_;
        int current_ = 0;
        uint64_t client_id_ = 0;
        uint64_t seq_ = 0;
    };

} 
