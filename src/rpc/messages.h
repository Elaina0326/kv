#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "common/codec.h"
#include "common/log_entry.h"
#include "common/types.h"

namespace raftkv
{

    const char *MsgTypeName(uint8_t type);

    enum class MsgType : uint8_t
    {
        kRequestVote = 1,
        kAppendEntries = 2,
        kClientGet = 10,
        kClientPut = 11,
        kClientDelete = 12,
        kClientScan = 13,
        kClientStatus = 14,
    };

    inline void PutStatus(Encoder *e, ErrorCode code, int leader_hint = 0)
    {
        e->PutU8(static_cast<uint8_t>(code));
        e->PutU32(static_cast<uint32_t>(leader_hint));
    }

    inline ErrorCode GetStatus(Decoder *d, int *leader_hint)
    {
        uint8_t code = 0;
        uint32_t hint = 0;
        if (!d->GetU8(&code) || !d->GetU32(&hint))
            return ErrorCode::kBadRequest;
        if (leader_hint != nullptr)
            *leader_hint = static_cast<int>(hint);
        return static_cast<ErrorCode>(code);
    }


    struct RequestVoteArgs
    {
        uint64_t term = 0;
        int candidate_id = 0;
        uint64_t last_log_index = 0;
        uint64_t last_log_term = 0;
    };

    struct RequestVoteReply
    {
        uint64_t term = 0;
        bool granted = false;
    };

    struct AppendEntriesArgs
    {
        uint64_t term = 0;
        int leader_id = 0;
        uint64_t prev_log_index = 0;
        uint64_t prev_log_term = 0;
        uint64_t leader_commit = 0;
        std::vector<LogEntry> entries;
    };

    struct AppendEntriesReply
    {
        uint64_t term = 0;
        bool success = false;
        uint64_t match_index = 0;

        uint64_t conflict_index = 0;
        uint64_t conflict_term = 0;
    };


    struct PutRequest
    {
        uint64_t client_id = 0;
        uint64_t seq = 0;
        std::string key;
        std::string value;
        uint64_t ttl_ms = 0; 
    };

    struct DeleteRequest
    {
        uint64_t client_id = 0;
        uint64_t seq = 0;
        std::string key;
    };

    struct GetRequest
    {
        std::string key;
    };

    struct ScanRequest
    {
        std::string start;
        std::string end; 
        uint32_t limit = 100;
    };

   

    inline std::string EncodeRequestVoteArgs(const RequestVoteArgs &a)
    {
        Encoder e;
        e.PutU64(a.term);
        e.PutU32(static_cast<uint32_t>(a.candidate_id));
        e.PutU64(a.last_log_index);
        e.PutU64(a.last_log_term);
        return e.TakeData();
    }

    inline bool DecodeRequestVoteArgs(const std::string &buf, RequestVoteArgs *a)
    {
        Decoder d(buf);
        uint32_t cid = 0;
        if (!d.GetU64(&a->term) || !d.GetU32(&cid) || !d.GetU64(&a->last_log_index) ||
            !d.GetU64(&a->last_log_term))
        {
            return false;
        }
        a->candidate_id = static_cast<int>(cid);
        return d.ok();
    }

    inline std::string EncodeRequestVoteReply(const RequestVoteReply &r)
    {
        Encoder e;
        PutStatus(&e, ErrorCode::kOk, 0);
        e.PutU64(r.term);
        e.PutBool(r.granted);
        return e.TakeData();
    }

    inline bool DecodeRequestVoteReply(const std::string &buf, RequestVoteReply *r)
    {
        Decoder d(buf);
        int hint = 0;
        GetStatus(&d, &hint);
        return d.GetU64(&r->term) && d.GetBool(&r->granted) && d.ok();
    }

    inline std::string EncodeAppendEntriesArgs(const AppendEntriesArgs &a)
    {
        Encoder e;
        e.PutU64(a.term);
        e.PutU32(static_cast<uint32_t>(a.leader_id));
        e.PutU64(a.prev_log_index);
        e.PutU64(a.prev_log_term);
        e.PutU64(a.leader_commit);
        e.PutU32(static_cast<uint32_t>(a.entries.size()));
        for (const auto &entry : a.entries)
        {
            e.PutU64(entry.index);
            e.PutU64(entry.term);
            e.PutU8(static_cast<uint8_t>(entry.type));
            e.PutString(entry.data);
        }
        return e.TakeData();
    }

    inline bool DecodeAppendEntriesArgs(const std::string &buf, AppendEntriesArgs *a)
    {
        Decoder d(buf);
        uint32_t lid = 0;
        uint32_t count = 0;
        if (!d.GetU64(&a->term) || !d.GetU32(&lid) || !d.GetU64(&a->prev_log_index) ||
            !d.GetU64(&a->prev_log_term) || !d.GetU64(&a->leader_commit) || !d.GetU32(&count))
        {
            return false;
        }
        a->leader_id = static_cast<int>(lid);
        if (count > 1u << 20)
            return false;
        a->entries.clear();
        a->entries.reserve(count);
        for (uint32_t i = 0; i < count; ++i)
        {
            LogEntry entry;
            uint8_t type = 0;
            if (!d.GetU64(&entry.index) || !d.GetU64(&entry.term) || !d.GetU8(&type) ||
                !d.GetString(&entry.data))
            {
                return false;
            }
            entry.type = static_cast<EntryType>(type);
            a->entries.push_back(std::move(entry));
        }
        return d.ok();
    }

    inline std::string EncodeAppendEntriesReply(const AppendEntriesReply &r)
    {
        Encoder e;
        PutStatus(&e, ErrorCode::kOk, 0);
        e.PutU64(r.term);
        e.PutBool(r.success);
        e.PutU64(r.match_index);
        e.PutU64(r.conflict_index);
        e.PutU64(r.conflict_term);
        return e.TakeData();
    }

    inline bool DecodeAppendEntriesReply(const std::string &buf, AppendEntriesReply *r)
    {
        Decoder d(buf);
        int hint = 0;
        GetStatus(&d, &hint);
        return d.GetU64(&r->term) && d.GetBool(&r->success) && d.GetU64(&r->match_index) &&
               d.GetU64(&r->conflict_index) && d.GetU64(&r->conflict_term) && d.ok();
    }


    inline std::string EncodePutRequest(const PutRequest &r)
    {
        Encoder e;
        e.PutU64(r.client_id);
        e.PutU64(r.seq);
        e.PutString(r.key);
        e.PutString(r.value);
        e.PutU64(r.ttl_ms);
        return e.TakeData();
    }

    inline bool DecodePutRequest(const std::string &buf, PutRequest *r)
    {
        Decoder d(buf);
        return d.GetU64(&r->client_id) && d.GetU64(&r->seq) && d.GetString(&r->key) &&
               d.GetString(&r->value) && d.GetU64(&r->ttl_ms) && d.ok();
    }

    inline std::string EncodeDeleteRequest(const DeleteRequest &r)
    {
        Encoder e;
        e.PutU64(r.client_id);
        e.PutU64(r.seq);
        e.PutString(r.key);
        return e.TakeData();
    }

    inline bool DecodeDeleteRequest(const std::string &buf, DeleteRequest *r)
    {
        Decoder d(buf);
        return d.GetU64(&r->client_id) && d.GetU64(&r->seq) && d.GetString(&r->key) && d.ok();
    }

    inline std::string EncodeGetRequest(const GetRequest &r)
    {
        Encoder e;
        e.PutString(r.key);
        return e.TakeData();
    }

    inline bool DecodeGetRequest(const std::string &buf, GetRequest *r)
    {
        Decoder d(buf);
        return d.GetString(&r->key) && d.ok();
    }

    inline std::string EncodeScanRequest(const ScanRequest &r)
    {
        Encoder e;
        e.PutString(r.start);
        e.PutString(r.end);
        e.PutU32(r.limit);
        return e.TakeData();
    }

    inline bool DecodeScanRequest(const std::string &buf, ScanRequest *r)
    {
        Decoder d(buf);
        return d.GetString(&r->start) && d.GetString(&r->end) && d.GetU32(&r->limit) && d.ok();
    }

    inline std::string EncodeValueReply(ErrorCode code, int leader_hint, const std::string &value)
    {
        Encoder e;
        PutStatus(&e, code, leader_hint);
        e.PutString(value);
        return e.TakeData();
    }

    inline std::string EncodeOkReply(ErrorCode code, int leader_hint)
    {
        Encoder e;
        PutStatus(&e, code, leader_hint);
        return e.TakeData();
    }

    inline std::string EncodeScanReply(ErrorCode code, int leader_hint,
                                       const std::vector<std::pair<std::string, std::string>> &kvs)
    {
        Encoder e;
        PutStatus(&e, code, leader_hint);
        e.PutU32(static_cast<uint32_t>(kvs.size()));
        for (const auto &kv : kvs)
        {
            e.PutString(kv.first);
            e.PutString(kv.second);
        }
        return e.TakeData();
    }

    inline bool DecodeScanReply(const std::string &buf, ErrorCode code, int *leader_hint,
                                std::vector<std::pair<std::string, std::string>> *kvs)
    {
        Decoder d(buf);
        const ErrorCode got = GetStatus(&d, leader_hint);
        if (got != ErrorCode::kOk || code != ErrorCode::kOk)
            return false;
        uint32_t count = 0;
        if (!d.GetU32(&count))
            return false;
        if (count > (1u << 20))
            return false;
        kvs->clear();
        kvs->reserve(count);
        for (uint32_t i = 0; i < count; ++i)
        {
            std::string k;
            std::string v;
            if (!d.GetString(&k) || !d.GetString(&v))
                return false;
            kvs->emplace_back(std::move(k), std::move(v));
        }
        return d.ok();
    }

   

    struct NodeStatus
    {
        int node_id = 0;
        uint8_t role = 0;
        uint64_t term = 0;
        uint64_t commit_index = 0;
        uint64_t last_applied = 0;
        uint64_t last_log_index = 0;
        int leader_id = 0;
        uint64_t key_count = 0;
        uint64_t applied_commands = 0;
    };

    inline std::string EncodeNodeStatusReply(const NodeStatus &s)
    {
        Encoder e;
        PutStatus(&e, ErrorCode::kOk, s.leader_id);
        e.PutU32(static_cast<uint32_t>(s.node_id));
        e.PutU8(s.role);
        e.PutU64(s.term);
        e.PutU64(s.commit_index);
        e.PutU64(s.last_applied);
        e.PutU64(s.last_log_index);
        e.PutU32(static_cast<uint32_t>(s.leader_id));
        e.PutU64(s.key_count);
        e.PutU64(s.applied_commands);
        return e.TakeData();
    }

    inline bool DecodeNodeStatusReply(const std::string &buf, NodeStatus *s)
    {
        Decoder d(buf);
        int hint = 0;
        if (GetStatus(&d, &hint) != ErrorCode::kOk)
            return false;
        uint32_t node_id = 0;
        uint32_t leader_id = 0;
        if (!d.GetU32(&node_id) || !d.GetU8(&s->role) || !d.GetU64(&s->term) ||
            !d.GetU64(&s->commit_index) || !d.GetU64(&s->last_applied) || !d.GetU64(&s->last_log_index) ||
            !d.GetU32(&leader_id) || !d.GetU64(&s->key_count) || !d.GetU64(&s->applied_commands))
        {
            return false;
        }
        s->node_id = static_cast<int>(node_id);
        s->leader_id = static_cast<int>(leader_id);
        return d.ok();
    }

} 
