#include "test_framework.h"

#include <string>
#include <vector>

#include "common/codec.h"
#include "common/crc32.h"
#include "rpc/messages.h"

using namespace raftkv;

TEST_CASE(EncoderIsBigEndian)
{
    Encoder e;
    e.PutU32(0x01020304u);
    e.PutU64(0x0102030405060708ull);
    const std::string &buf = e.data();
    EXPECT_EQ(size_t{12}, buf.size());

    EXPECT_EQ(0x01, static_cast<uint8_t>(buf[0]));
    EXPECT_EQ(0x04, static_cast<uint8_t>(buf[3]));
    EXPECT_EQ(0x01, static_cast<uint8_t>(buf[4]));
    EXPECT_EQ(0x08, static_cast<uint8_t>(buf[11]));
}

TEST_CASE(EncoderRoundTripAllPrimitives)
{
    Encoder e;
    e.PutU8(7);
    e.PutU16(65530);
    e.PutU32(4000000000u);
    e.PutU64(0xFFFFFFFFFFFFFFF0ull);
    e.PutBool(true);
    e.PutBool(false);
    e.PutString("hello 世界");
    e.PutString(""); // 空串也要能编码
    e.PutRaw("\x00\x01\x02", 3);
    const std::string buf = e.TakeData();

    Decoder d(buf);
    uint8_t u8 = 0;
    uint16_t u16 = 0;
    uint32_t u32 = 0;
    uint64_t u64 = 0;
    bool b1 = false;
    bool b2 = true;
    std::string s1;
    std::string s2;
    std::string raw(3, '\0');
    EXPECT_TRUE(d.GetU8(&u8));
    EXPECT_TRUE(d.GetU16(&u16));
    EXPECT_TRUE(d.GetU32(&u32));
    EXPECT_TRUE(d.GetU64(&u64));
    EXPECT_TRUE(d.GetBool(&b1));
    EXPECT_TRUE(d.GetBool(&b2));
    EXPECT_TRUE(d.GetString(&s1));
    EXPECT_TRUE(d.GetString(&s2));
    EXPECT_TRUE(d.GetRaw(raw.data(), 3));
    EXPECT_EQ(uint8_t{7}, u8);
    EXPECT_EQ(uint16_t{65530}, u16);
    EXPECT_EQ(uint32_t{4000000000u}, u32);
    EXPECT_EQ(0xFFFFFFFFFFFFFFF0ull, u64);
    EXPECT_TRUE(b1);
    EXPECT_FALSE(b2);
    EXPECT_EQ(std::string("hello 世界"), s1);
    EXPECT_EQ(std::string(""), s2);
    EXPECT_EQ(size_t{3}, raw.size());
    EXPECT_EQ(uint8_t{0}, static_cast<uint8_t>(raw[0]));
    EXPECT_EQ(uint8_t{2}, static_cast<uint8_t>(raw[2]));
    EXPECT_TRUE(d.ok());
    EXPECT_EQ(size_t{0}, d.remaining());
}

TEST_CASE(DecoderRejectsTruncatedInput)
{
    Encoder e;
    e.PutU64(42);
    e.PutString("payload");
    const std::string full = e.TakeData();

    for (size_t len = 0; len < full.size(); ++len)
    {
        Decoder d(std::string_view(full).substr(0, len));
        uint64_t v = 0;
        std::string s;
        const bool got_all = d.GetU64(&v) && d.GetString(&s) && d.ok();
        EXPECT_FALSE(got_all);
        EXPECT_FALSE(d.ok()); 
    }
}

TEST_CASE(FrameLayoutAndOversizeStringIsSafe)
{
    const std::string frame = EncodeFrame(static_cast<uint8_t>(MsgType::kClientPut), "abcd");
    ASSERT_TRUE(frame.size() == 4 + 1 + 4);

    EXPECT_EQ(uint8_t{0x00}, static_cast<uint8_t>(frame[0]));
    EXPECT_EQ(uint8_t{0x05}, static_cast<uint8_t>(frame[3]));
    EXPECT_EQ(uint8_t{11}, static_cast<uint8_t>(frame[4]));


    Encoder e;
    e.PutU32(1024 * 1024);
    e.PutRaw("abc", 3);
    const std::string evil = e.TakeData();
    Decoder d(evil);
    std::string out;
    EXPECT_FALSE(d.GetString(&out));
}

TEST_CASE(Crc32KnownVector)
{

    const char *s = "123456789";
    EXPECT_EQ(0xCBF43926u, Crc32(s, 9));
    EXPECT_EQ(0u, Crc32("", 0));
}

TEST_CASE(AppendEntriesArgsRoundTrip)
{
    AppendEntriesArgs args;
    args.term = 12;
    args.leader_id = 3;
    args.prev_log_index = 100;
    args.prev_log_term = 11;
    args.leader_commit = 99;
    LogEntry first;
    first.index = 101;
    first.term = 12;
    first.type = EntryType::kCommand;
    first.data = "put name elaina";
    LogEntry second;
    second.index = 102;
    second.term = 12;
    second.type = EntryType::kNoop;
    args.entries = {first, second};

    AppendEntriesArgs decoded;
    ASSERT_TRUE(DecodeAppendEntriesArgs(EncodeAppendEntriesArgs(args), &decoded));
    EXPECT_EQ(uint64_t{12}, decoded.term);
    EXPECT_EQ(3, decoded.leader_id);
    EXPECT_EQ(uint64_t{100}, decoded.prev_log_index);
    EXPECT_EQ(uint64_t{11}, decoded.prev_log_term);
    EXPECT_EQ(uint64_t{99}, decoded.leader_commit);
    ASSERT_TRUE(decoded.entries.size() == 2);
    EXPECT_EQ(uint64_t{101}, decoded.entries[0].index);
    EXPECT_EQ(std::string("put name elaina"), decoded.entries[0].data);
    EXPECT_EQ(EntryType::kCommand, decoded.entries[0].type);
    EXPECT_EQ(EntryType::kNoop, decoded.entries[1].type);
}

TEST_CASE(ReplyStatusHeaderRoundTrip)
{
    std::string reply = EncodeOkReply(ErrorCode::kNotLeader, 2);
    Decoder d(reply);
    int hint = 0;
    EXPECT_EQ(ErrorCode::kNotLeader, GetStatus(&d, &hint));
    EXPECT_EQ(2, hint);

    reply = EncodeValueReply(ErrorCode::kOk, 1, "value");
    Decoder d2(reply);
    EXPECT_EQ(ErrorCode::kOk, GetStatus(&d2, &hint));
    EXPECT_EQ(1, hint);
    std::string value;
    EXPECT_TRUE(d2.GetString(&value));
    EXPECT_EQ(std::string("value"), value);

    NodeStatus status;
    status.node_id = 2;
    status.role = 2;
    status.term = 9;
    status.commit_index = 88;
    status.last_applied = 88;
    status.last_log_index = 88;
    status.leader_id = 2;
    status.key_count = 1000;
    status.applied_commands = 1000;
    NodeStatus out;
    ASSERT_TRUE(DecodeNodeStatusReply(EncodeNodeStatusReply(status), &out));
    EXPECT_EQ(2, out.node_id);
    EXPECT_EQ(uint8_t{2}, out.role);
    EXPECT_EQ(uint64_t{9}, out.term);
    EXPECT_EQ(uint64_t{1000}, out.key_count);
    EXPECT_EQ(2, out.leader_id);
}
