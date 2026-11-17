#include "rpc/messages.h"

namespace raftkv
{

    const char *MsgTypeName(uint8_t type)
    {
        switch (static_cast<MsgType>(type))
        {
        case MsgType::kRequestVote:
            return "RequestVote";
        case MsgType::kAppendEntries:
            return "AppendEntries";
        case MsgType::kClientGet:
            return "ClientGet";
        case MsgType::kClientPut:
            return "ClientPut";
        case MsgType::kClientDelete:
            return "ClientDelete";
        case MsgType::kClientScan:
            return "ClientScan";
        case MsgType::kClientStatus:
            return "ClientStatus";
        }
        return "Unknown";
    }

} 
