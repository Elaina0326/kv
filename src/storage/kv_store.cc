#include "storage/kv_store.h"

#include "common/codec.h"
#include "common/util.h"

namespace raftkv
{

    std::string EncodeCommand(const Command &cmd)
    {
        Encoder e;
        e.PutU8(static_cast<uint8_t>(cmd.op));
        e.PutU64(cmd.client_id);
        e.PutU64(cmd.seq);
        e.PutString(cmd.key);
        e.PutString(cmd.value);
        e.PutU64(cmd.expire_at_ms);
        return e.TakeData();
    }

    bool DecodeCommand(const std::string &data, Command *cmd)
    {
        Decoder d(data);
        uint8_t op = 0;
        if (!d.GetU8(&op) || !d.GetU64(&cmd->client_id) || !d.GetU64(&cmd->seq) ||
            !d.GetString(&cmd->key) || !d.GetString(&cmd->value) || !d.GetU64(&cmd->expire_at_ms))
        {
            return false;
        }
        if (op != static_cast<uint8_t>(Command::Op::kPut) &&
            op != static_cast<uint8_t>(Command::Op::kDelete))
        {
            return false;
        }
        cmd->op = static_cast<Command::Op>(op);
        return d.ok();
    }

    ErrorCode KVStore::Apply(const std::string &command_data)
    {
        Command cmd;
        if (!DecodeCommand(command_data, &cmd))
            return ErrorCode::kBadRequest;

        std::lock_guard<std::mutex> lock(mu_);
     
        auto it = last_seq_.find(cmd.client_id);
        if (it != last_seq_.end() && cmd.seq <= it->second)
        {
            ++dedup_hits_;
            return ErrorCode::kOk;
        }
        last_seq_[cmd.client_id] = cmd.seq;

       
        if (cmd.op == Command::Op::kPut)
        {
            Value v;
            v.data = std::move(cmd.value);
            v.expire_at_ms = static_cast<int64_t>(cmd.expire_at_ms);
            map_[cmd.key] = std::move(v);
        }
        else
        {
            map_.erase(cmd.key);
        }
        ++applied_commands_;
        return ErrorCode::kOk;
    }

    ErrorCode KVStore::Get(const std::string &key, std::string *value)
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = map_.find(key);
        if (it == map_.end())
            return ErrorCode::kNotFound;
        if (IsExpired(it->second, WallMs()))
        { 
            map_.erase(it);
            return ErrorCode::kNotFound;
        }
        *value = it->second.data;
        return ErrorCode::kOk;
    }

    ErrorCode KVStore::Scan(const std::string &start, const std::string &end, uint32_t limit,
                            std::vector<std::pair<std::string, std::string>> *out,
                            size_t max_bytes)
    {
        std::lock_guard<std::mutex> lock(mu_);
        const int64_t now = WallMs();
        if (limit == 0)
            limit = 100;
        size_t used = 0;
        auto it = map_.lower_bound(start);
        for (; it != map_.end() && out->size() < limit; ++it)
        {
            if (!end.empty() && it->first >= end)
                break;
            if (IsExpired(it->second, now))
                continue;
            const size_t cost = it->first.size() + it->second.data.size() + 8;
    
            if (max_bytes > 0 && !out->empty() && used + cost > max_bytes)
                break;
            used += cost;
            out->emplace_back(it->first, it->second.data);
        }
        return ErrorCode::kOk;
    }

    size_t KVStore::size() const
    {
        std::lock_guard<std::mutex> lock(mu_);
        return map_.size();
    }

    size_t KVStore::SweepExpired()
    {
        std::lock_guard<std::mutex> lock(mu_);
        const int64_t now = WallMs();
        size_t removed = 0;
        for (auto it = map_.begin(); it != map_.end();)
        {
            if (IsExpired(it->second, now))
            {
                it = map_.erase(it);
                ++removed;
            }
            else
            {
                ++it;
            }
        }
        return removed;
    }

} 
