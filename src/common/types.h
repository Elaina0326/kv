#pragma once

#include <cstdint>
#include <string>

namespace raftkv
{

    enum class ErrorCode : uint8_t
    {
        kOk = 0,
        kNotLeader = 1,  
        kTimeout = 2,     
        kNotFound = 3,   
        kBadRequest = 4,  
        kInternal = 5,   
        kUnavailable = 6,
    };

    inline const char *ErrorCodeName(ErrorCode code)
    {
        switch (code)
        {
        case ErrorCode::kOk:
            return "OK";
        case ErrorCode::kNotLeader:
            return "NOT_LEADER";
        case ErrorCode::kTimeout:
            return "TIMEOUT";
        case ErrorCode::kNotFound:
            return "NOT_FOUND";
        case ErrorCode::kBadRequest:
            return "BAD_REQUEST";
        case ErrorCode::kInternal:
            return "INTERNAL";
        case ErrorCode::kUnavailable:
            return "UNAVAILABLE";
        }
        return "UNKNOWN";
    }


    struct Status
    {
        ErrorCode code = ErrorCode::kOk;
        std::string msg;

        bool ok() const { return code == ErrorCode::kOk; }
        static Status Ok() { return Status{}; }
        static Status Error(ErrorCode c, std::string m = "") { return Status{c, std::move(m)}; }
    };

} 
