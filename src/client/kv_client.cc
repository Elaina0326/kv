#include "client/kv_client.h"

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "common/log.h"
#include "common/util.h"

namespace raftkv
{

    namespace
    {
        std::atomic<uint64_t> g_client_counter{0};
    } 

    KVClient::KVClient(std::vector<std::string> addrs, int timeout_ms)
        : addrs_(std::move(addrs)), rpc_(timeout_ms), timeout_ms_(timeout_ms)
    {
       
        const uint64_t counter = ++g_client_counter;
        client_id_ = (static_cast<uint64_t>(::getpid()) << 40) ^
                     ((static_cast<uint64_t>(NowMs()) & 0xFFFFFF) << 16) ^ (counter & 0xFFFF);
        if (addrs_.empty())
            addrs_.push_back("127.0.0.1:9201");
    }

    bool KVClient::Invoke(uint8_t type, const std::string &payload, std::string *reply,
                          ErrorCode *code)
    {
        const int n = static_cast<int>(addrs_.size());
        int idx = current_ % n;
        for (int attempt = 0; attempt < 10; ++attempt)
        {
            if (idx < 0 || idx >= n)
                idx = 0;
            std::string buf;
            if (!rpc_.Call(addrs_[idx], type, payload, &buf))
            {
                idx = (idx + 1) % n;
                continue;
            }
            Decoder decoder(buf);
            int hint = 0;
            const ErrorCode got = GetStatus(&decoder, &hint);
            if (got == ErrorCode::kNotLeader)
            {
                if (hint >= 1 && hint <= n && hint - 1 != idx)
                {
                    idx = hint - 1;
                }
                else
                {
                    idx = (idx + 1) % n;
                }
             
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                continue;
            }
            if (got == ErrorCode::kTimeout)
            {
      
                if (attempt + 1 < 10)
                {
                    idx = (idx + 1) % n;
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    continue;
                }
      
                current_ = idx;
                *reply = std::move(buf);
                *code = got;
                return true;
            }
            if (got == ErrorCode::kUnavailable)
            {
      
                if (attempt + 1 < 10)
                {
                    idx = (idx + 1) % n;
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                    continue;
                }
                current_ = idx;
                *reply = std::move(buf);
                *code = got;
                return true;
            }
            current_ = idx;
            *reply = std::move(buf);
            *code = got;
            return true;
        }
        *code = ErrorCode::kUnavailable;
        return false;
    }

    ErrorCode KVClient::InvokeWithRetry(uint8_t type, const std::string &payload,
                                        std::string *reply)
    {

        const int64_t backoff_ms[] = {20, 50, 100};
        ErrorCode code = ErrorCode::kUnavailable;
        for (size_t attempt = 0; attempt <= sizeof(backoff_ms) / sizeof(backoff_ms[0]); ++attempt)
        {
            if (attempt > 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms[attempt - 1]));
            }
            if (!Invoke(type, payload, reply, &code))
                return code; 
    
            if (code != ErrorCode::kTimeout && code != ErrorCode::kUnavailable)
                return code;
        }
        return code;
    }

    ErrorCode KVClient::Put(const std::string &key, const std::string &value, uint64_t ttl_ms)
    {
        PutRequest req;
        req.client_id = client_id_;
        req.seq = ++seq_; 
        req.key = key;
        req.value = value;
        req.ttl_ms = ttl_ms;

        std::string reply;
        return InvokeWithRetry(static_cast<uint8_t>(MsgType::kClientPut), EncodePutRequest(req), &reply);
    }

    ErrorCode KVClient::Delete(const std::string &key)
    {
        DeleteRequest req;
        req.client_id = client_id_;
        req.seq = ++seq_;
        req.key = key;

        std::string reply;
        return InvokeWithRetry(static_cast<uint8_t>(MsgType::kClientDelete), EncodeDeleteRequest(req),
                               &reply);
    }

    ErrorCode KVClient::Get(const std::string &key, std::string *value)
    {
        GetRequest req;
        req.key = key;

        std::string reply;
        const ErrorCode code =
            InvokeWithRetry(static_cast<uint8_t>(MsgType::kClientGet), EncodeGetRequest(req), &reply);
        if (code != ErrorCode::kOk)
            return code;
        Decoder decoder(reply);
        GetStatus(&decoder, nullptr);
        if (!decoder.GetString(value) || !decoder.ok())
            return ErrorCode::kBadRequest;
        return ErrorCode::kOk;
    }

    ErrorCode KVClient::Scan(const std::string &start, const std::string &end, uint32_t limit,
                             std::vector<std::pair<std::string, std::string>> *out)
    {
        ScanRequest req;
        req.start = start;
        req.end = end;
        req.limit = limit;

        std::string reply;
        const ErrorCode code =
            InvokeWithRetry(static_cast<uint8_t>(MsgType::kClientScan), EncodeScanRequest(req), &reply);
        if (code != ErrorCode::kOk)
            return code;
        int hint = 0;
        if (!DecodeScanReply(reply, ErrorCode::kOk, &hint, out))
            return ErrorCode::kBadRequest;
        return ErrorCode::kOk;
    }

    ErrorCode KVClient::Status(NodeStatus *status, int node_index)
    {
        if (node_index < 0 || node_index >= static_cast<int>(addrs_.size()))
            return ErrorCode::kBadRequest;
        std::string reply;
        if (!rpc_.Call(addrs_[node_index], static_cast<uint8_t>(MsgType::kClientStatus), "", &reply))
        {
            return ErrorCode::kUnavailable;
        }
        if (!DecodeNodeStatusReply(reply, status))
            return ErrorCode::kBadRequest;
        return ErrorCode::kOk;
    }

} 
