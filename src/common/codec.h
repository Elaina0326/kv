#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace raftkv
{

    class Encoder
    {
    public:
        void PutU8(uint8_t v) { buf_.push_back(static_cast<char>(v)); }
        void PutU16(uint16_t v) { PutBE(v, 2); }
        void PutU32(uint32_t v) { PutBE(v, 4); }
        void PutU64(uint64_t v) { PutBE(v, 8); }
        void PutBool(bool v) { PutU8(v ? 1 : 0); }

        void PutString(std::string_view s)
        {
            assert(s.size() <= 0xFFFFFFFFu);
            PutU32(static_cast<uint32_t>(s.size()));
            buf_.append(s.data(), s.size());
        }

        void PutRaw(const void *data, size_t len)
        {
            buf_.append(static_cast<const char *>(data), len);
        }

        const std::string &data() const { return buf_; }
        std::string TakeData() { return std::move(buf_); }
        size_t size() const { return buf_.size(); }
        void Clear() { buf_.clear(); }

    private:
        void PutBE(uint64_t v, int bytes)
        {
            for (int i = bytes - 1; i >= 0; --i)
                buf_.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
        }
        std::string buf_;
    };

    class Decoder
    {
    public:
        explicit Decoder(std::string_view data) : data_(data) {}

        bool GetU8(uint8_t *v)
        {
            if (!Need(1))
                return false;
            *v = static_cast<uint8_t>(data_[pos_]);
            pos_ += 1;
            return true;
        }
        bool GetU16(uint16_t *v)
        {
            uint64_t tmp = 0;
            if (!GetBE(&tmp, 2))
                return false;
            *v = static_cast<uint16_t>(tmp);
            return true;
        }
        bool GetU32(uint32_t *v)
        {
            uint64_t tmp = 0;
            if (!GetBE(&tmp, 4))
                return false;
            *v = static_cast<uint32_t>(tmp);
            return true;
        }
        bool GetU64(uint64_t *v) { return GetBE(v, 8); }
        bool GetBool(bool *v)
        {
            uint8_t b = 0;
            if (!GetU8(&b))
                return false;
            *v = (b != 0);
            return true;
        }

        bool GetString(std::string *out)
        {
            uint32_t len = 0;
            if (!GetU32(&len))
                return false;
            if (!Need(len))
                return false;
            out->assign(data_.data() + pos_, len);
            pos_ += len;
            return true;
        }

        bool GetRaw(void *out, size_t len)
        {
            if (!Need(len))
                return false;
            std::memcpy(out, data_.data() + pos_, len);
            pos_ += len;
            return true;
        }

        bool GetView(size_t len, std::string_view *out)
        {
            if (!Need(len))
                return false;
            *out = data_.substr(pos_, len);
            pos_ += len;
            return true;
        }

        bool ok() const { return ok_; }
        size_t remaining() const { return data_.size() - pos_; }
        size_t position() const { return pos_; }

    private:
        bool Need(size_t n)
        {
            if (!ok_)
                return false;
            if (data_.size() - pos_ < n)
            {
                ok_ = false;
                return false;
            }
            return true;
        }
        bool GetBE(uint64_t *v, int bytes)
        {
            if (!Need(static_cast<size_t>(bytes)))
                return false;
            uint64_t out = 0;
            for (int i = 0; i < bytes; ++i)
            {
                out = (out << 8) | static_cast<uint8_t>(data_[pos_ + i]);
            }
            pos_ += static_cast<size_t>(bytes);
            *v = out;
            return true;
        }

        std::string_view data_;
        size_t pos_ = 0;
        bool ok_ = true;
    };

    inline constexpr size_t kFrameHeaderSize = 5;
    inline constexpr uint32_t kMaxFrameSize = 64u * 1024 * 1024; // 64MB，防恶意长度打爆内存

    inline std::string EncodeFrame(uint8_t type, std::string_view payload)
    {
        assert(payload.size() + 1 <= kMaxFrameSize);
        assert(payload.size() + 1 <= 0xFFFFFFFFu); 
        std::string out;
        uint32_t len = static_cast<uint32_t>(payload.size() + 1);
        out.reserve(4 + len);
        out.push_back(static_cast<char>((len >> 24) & 0xFF));
        out.push_back(static_cast<char>((len >> 16) & 0xFF));
        out.push_back(static_cast<char>((len >> 8) & 0xFF));
        out.push_back(static_cast<char>(len & 0xFF));
        out.push_back(static_cast<char>(type));
        out.append(payload.data(), payload.size());
        return out;
    }

} 
