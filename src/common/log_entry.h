#pragma once

#include <cstdint>
#include <string>

namespace raftkv
{

    enum class EntryType : uint8_t
    {
        kCommand = 1, 
        kNoop = 2,   
    };

    
    struct LogEntry
    {
        uint64_t index = 0;
        uint64_t term = 0;
        EntryType type = EntryType::kCommand;
        std::string data;

        bool operator==(const LogEntry &o) const
        {
            return index == o.index && term == o.term && type == o.type && data == o.data;
        }
    };

} 
