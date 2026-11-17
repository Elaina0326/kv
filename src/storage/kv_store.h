#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/types.h"

namespace raftkv
{

    struct Command
    {
        enum class Op : uint8_t
        {
            kPut = 1,
            kDelete = 2
        };

        Op op = Op::kPut;
        uint64_t client_id = 0;
        uint64_t seq = 0;
        std::string key;
        std::string value;

        uint64_t expire_at_ms = 0;
    };

    std::string EncodeCommand(const Command &cmd);
    bool DecodeCommand(const std::string &data, Command *cmd);

    class KVStore
    {
    public:

        ErrorCode Apply(const std::string &command_data);


        ErrorCode Get(const std::string &key, std::string *value);
     
        ErrorCode Scan(const std::string &start, const std::string &end, uint32_t limit,
                       std::vector<std::pair<std::string, std::string>> *out, size_t max_bytes = 0);

        size_t size() const;
        size_t SweepExpired();
        uint64_t applied_commands() const { return applied_commands_.load(std::memory_order_relaxed); }
        uint64_t dedup_hits() const { return dedup_hits_.load(std::memory_order_relaxed); }

    private:
        struct Value
        {
            std::string data;
            int64_t expire_at_ms = 0; 
        };

        bool IsExpired(const Value &v, int64_t now_ms) const
        {
            return v.expire_at_ms > 0 && now_ms >= v.expire_at_ms;
        }

        mutable std::mutex mu_;
        std::map<std::string, Value> map_; 
        std::unordered_map<uint64_t, uint64_t> last_seq_;
        std::atomic<uint64_t> applied_commands_{0};
        std::atomic<uint64_t> dedup_hits_{0};
    };

}
