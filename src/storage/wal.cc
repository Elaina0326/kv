#include "storage/wal.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstring>

#include "common/codec.h"
#include "common/crc32.h"
#include "common/log.h"
#include "common/util.h"

namespace raftkv
{

    namespace
    {
        constexpr char kMagic[8] = {'R', 'K', 'V', 'W', 'A', 'L', '1', '\0'};
        constexpr uint32_t kVersion = 2;
        constexpr size_t kHeaderSize = 16;
        constexpr uint32_t kMaxPayload = 64u * 1024 * 1024;

        bool WriteExact(int fd, const void *data, size_t len)
        {
            const char *p = static_cast<const char *>(data);
            size_t done = 0;
            while (done < len)
            {
                ssize_t n = ::write(fd, p + done, len - done);
                if (n > 0)
                {
                    done += static_cast<size_t>(n);
                    continue;
                }
                if (n < 0 && errno == EINTR)
                    continue;
                return false;
            }
            return true;
        }

        enum class ReadResult
        {
            kOk,
            kEof,
            kError
        };

       
        std::atomic<int> g_read_fault{0};

        bool TakeReadFault()
        {
            if (g_read_fault.load() <= 0)
                return false;
            g_read_fault.fetch_sub(1);
            return true;
        }

        ReadResult ReadExact(int fd, void *data, size_t len)
        {
            if (TakeReadFault())
            {
                errno = EIO;
                return ReadResult::kError;
            }
            char *p = static_cast<char *>(data);
            size_t done = 0;
            while (done < len)
            {
                ssize_t n = ::read(fd, p + done, len - done);
                if (n > 0)
                {
                    done += static_cast<size_t>(n);
                    continue;
                }
                if (n == 0)
                    return ReadResult::kEof; 
                if (errno == EINTR)
                    continue;
                return ReadResult::kError;
            }
            return ReadResult::kOk;
        }

        std::string ParentDir(const std::string &path)
        {
            const size_t pos = path.find_last_of('/');
            if (pos == std::string::npos)
                return ".";
            if (pos == 0)
                return "/";
            return path.substr(0, pos);
        }

    
        void SyncParentDir(const std::string &path)
        {
            const std::string dir = ParentDir(path);
            const int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
            if (dfd < 0)
            {
                LOG_WARN("open dir %s failed: %s", dir.c_str(), ErrnoString().c_str());
                return;
            }
            if (::fsync(dfd) != 0)
                LOG_WARN("fsync dir %s failed: %s", dir.c_str(), ErrnoString().c_str());
            ::close(dfd);
        }
    } 

    Wal::~Wal() { Close(); }

    bool Wal::Open(const std::string &path, bool sync, std::string *err)
    {
        path_ = path;
        sync_ = sync;
        fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
        if (fd_ < 0)
        {
            *err = "open " + path + " failed: " + ErrnoString();
            return false;
        }
        struct stat st{};
        if (::fstat(fd_, &st) != 0)
        {
            *err = "fstat failed: " + ErrnoString();
            return false;
        }
        if (st.st_size == 0)
        {
            if (!WriteHeader(err))
                return false;
            SyncParentDir(path_); 
        }
        else if (static_cast<size_t>(st.st_size) < kHeaderSize)
        {
            LOG_WARN("wal %s: header truncated (%lld of %zu bytes), rebuilding empty wal", path_.c_str(),
                     static_cast<long long>(st.st_size), kHeaderSize);
            if (::ftruncate(fd_, 0) != 0)
            {
                *err = "truncate broken wal header failed: " + ErrnoString();
                return false;
            }
            if (!WriteHeader(err))
                return false;
            SyncParentDir(path_);
        }
        else
        {
            char magic[8];
            uint32_t version = 0;
            uint32_t reserved = 0;
            if (ReadExact(fd_, magic, 8) != ReadResult::kOk || std::memcmp(magic, kMagic, 8) != 0)
            {
                *err = "wal bad magic: " + path;
                return false;
            }
            if (ReadExact(fd_, &version, 4) != ReadResult::kOk ||
                ReadExact(fd_, &reserved, 4) != ReadResult::kOk || version != kVersion)
            {
                *err = "wal unsupported version (expect " + std::to_string(kVersion) + ", got " +
                       std::to_string(version) + "): " + path + "，请删除旧的 data 目录后重启";
                return false;
            }
        }
        end_offset_ = kHeaderSize;
        return true;
    }

    bool Wal::WriteHeader(std::string *err)
    {
        char buf[kHeaderSize] = {0};
        std::memcpy(buf, kMagic, 8);
        std::memcpy(buf + 8, &kVersion, 4);
        if (!WriteExact(fd_, buf, sizeof(buf)))
        {
            *err = "write wal header failed: " + ErrnoString();
            return false;
        }
        if (::fdatasync(fd_) != 0)
        {
            *err = "fsync wal header failed: " + ErrnoString();
            return false;
        }
        return true;
    }

    bool Wal::AppendRecord(uint8_t type, const std::string &payload, bool force_sync, std::string *err)
    {
        if (fd_ < 0)
        {
            *err = "wal not open";
            return false;
        }
        if (failed_)
        {
            *err = "wal is in failed state: " + failure_reason_;
            return false;
        }

        std::string record(8, '\0');
        record.push_back(static_cast<char>(type));
        record.append(payload);
        const uint32_t len = static_cast<uint32_t>(record.size() - 8);
        const uint32_t crc = Crc32(record.data() + 8, len);
        record[0] = static_cast<char>((len >> 24) & 0xFF);
        record[1] = static_cast<char>((len >> 16) & 0xFF);
        record[2] = static_cast<char>((len >> 8) & 0xFF);
        record[3] = static_cast<char>(len & 0xFF);
        std::memcpy(record.data() + 4, &crc, 4);

        if (!WriteExact(fd_, record.data(), record.size()))
        {
            const std::string why = "wal append failed: " + ErrnoString();

            if (end_offset_ > 0 && ::ftruncate(fd_, static_cast<off_t>(end_offset_)) != 0)
            {
                LOG_ERROR("wal %s: rollback to %llu failed: %s", path_.c_str(),
                          static_cast<unsigned long long>(end_offset_), ErrnoString().c_str());
            }
            failed_ = true;
            failure_reason_ = why;
            *err = why;
            return false;
        }
        end_offset_ += record.size();

        if ((force_sync || sync_) && ::fdatasync(fd_) != 0)
        {

            const std::string why = "wal fsync failed: " + ErrnoString();
            failed_ = true;
            failure_reason_ = why;
            *err = why;
            return false;
        }
        return true;
    }

    bool Wal::AppendMeta(uint64_t term, uint64_t voted_for, std::string *err)
    {
        Encoder e;
        e.PutU64(term);
        e.PutU64(voted_for);

        if (!AppendRecord(kMeta, e.data(), true, err))
            return false;
        term_ = term;
        voted_for_ = voted_for;
        return true;
    }

    bool Wal::AppendEntry(const LogEntry &entry, std::string *err)
    {
        Encoder e;
        e.PutU64(entry.index);
        e.PutU64(entry.term);
        e.PutU8(static_cast<uint8_t>(entry.type));
        e.PutString(entry.data);
        if (!AppendRecord(kEntry, e.data(), false, err))
            return false;
        entries_.push_back(entry);
        return true;
    }

    bool Wal::AppendTruncate(uint64_t from_index, std::string *err)
    {
        Encoder e;
        e.PutU64(from_index);
        if (!AppendRecord(kTruncate, e.data(), false, err))
            return false;
        while (!entries_.empty() && entries_.back().index >= from_index)
            entries_.pop_back();
        if (from_index > 0 && commit_index_ >= from_index)
            commit_index_ = from_index - 1;
        return true;
    }

    bool Wal::AppendCommit(uint64_t commit_index, std::string *err)
    {
        Encoder e;
        e.PutU64(commit_index);
        if (!AppendRecord(kCommit, e.data(), false, err))
            return false;
        commit_index_ = commit_index;
        return true;
    }

    bool Wal::Load(std::string *err)
    {
        if (fd_ < 0)
        {
            *err = "wal not open";
            return false;
        }
        if (failed_)
        {
            *err = "wal is in failed state: " + failure_reason_;
            return false;
        }
        struct stat st{};
        if (::fstat(fd_, &st) != 0)
        {
            *err = "wal fstat failed: " + ErrnoString();
            return false;
        }
        const uint64_t file_size = static_cast<uint64_t>(st.st_size);
        if (::lseek(fd_, kHeaderSize, SEEK_SET) < 0)
        {
            *err = "wal seek failed: " + ErrnoString();
            return false;
        }

        uint64_t offset = kHeaderSize;
        uint64_t good_offset = offset;
        entries_.clear();
        term_ = 0;
        voted_for_ = 0;
        commit_index_ = 0;


        const auto at_tail = [&](uint64_t len)
        { return offset + 8 + len >= file_size; };
        const auto bad_tail = [&](const char *what, uint64_t len)
        {
            LOG_WARN("wal %s: %s at tail offset %llu (file size %llu), truncating %llu bytes", path_.c_str(),
                     what, static_cast<unsigned long long>(offset),
                     static_cast<unsigned long long>(file_size),
                     static_cast<unsigned long long>(file_size - offset));
            (void)len;
        };

        while (true)
        {
            char header[8];
            ssize_t n = 0;
            if (TakeReadFault())
            {
                errno = EIO;
                n = -1;
            }
            else
            {
                n = ::read(fd_, header, sizeof(header));
            }
            if (n == 0)
                break; 
            if (n < 0 && errno == EINTR)
                continue;
            if (n < 0)
            {

                *err = "wal read failed at offset " + std::to_string(offset) + ": " + ErrnoString() +
                       "，拒绝启动（不截断文件，请先检查磁盘/文件）";
                LOG_ERROR("%s", err->c_str());
                return false;
            }
            if (static_cast<size_t>(n) != sizeof(header))
            { 
                LOG_WARN("wal %s: truncated tail at offset %llu, recovering", path_.c_str(),
                         static_cast<unsigned long long>(offset));
                break;
            }
            uint32_t len = (static_cast<uint32_t>(static_cast<unsigned char>(header[0])) << 24) |
                           (static_cast<uint32_t>(static_cast<unsigned char>(header[1])) << 16) |
                           (static_cast<uint32_t>(static_cast<unsigned char>(header[2])) << 8) |
                           static_cast<uint32_t>(static_cast<unsigned char>(header[3]));
            uint32_t crc = 0;
            std::memcpy(&crc, header + 4, 4);
            if (len == 0 || len > kMaxPayload)
            {

                if (offset + 8 >= file_size)
                {
                    bad_tail("bad record len", len);
                    break;
                }
                *err = "wal corrupted in the middle at offset " + std::to_string(offset) + " (file size " +
                       std::to_string(file_size) + ", last good offset " + std::to_string(good_offset) +
                       "): bad record length " + std::to_string(len) +
                       "，拒绝启动（请从其它副本重建该节点）";
                LOG_ERROR("%s", err->c_str());
                return false;
            }
            std::string body(len, '\0');
            const ReadResult rr = ReadExact(fd_, body.data(), len);
            if (rr == ReadResult::kError)
            {
  
                *err = "wal read failed at offset " + std::to_string(offset) + " (payload " +
                       std::to_string(len) + " bytes): " + ErrnoString() +
                       "，拒绝启动（不截断文件，请先检查磁盘/文件）";
                LOG_ERROR("%s", err->c_str());
                return false;
            }
            if (rr == ReadResult::kEof)
            { 
                LOG_WARN("wal %s: short record at offset %llu, recovering", path_.c_str(),
                         static_cast<unsigned long long>(offset));
                break;
            }
            if (Crc32(body.data(), body.size()) != crc)
            {

                if (!at_tail(len))
                {
                    *err = "wal corrupted in the middle at offset " + std::to_string(offset) + " (file size " +
                           std::to_string(file_size) + ", last good offset " + std::to_string(good_offset) +
                           "): crc mismatch，拒绝启动（请从其它副本重建该节点）";
                    LOG_ERROR("%s", err->c_str());
                    return false;
                }
                bad_tail("crc mismatch", len);
                break;
            }

            const uint8_t type = static_cast<uint8_t>(body[0]);
            Decoder d(std::string_view(body).substr(1));
            bool ok = true;
            if (type == kMeta)
            {
                uint64_t t = 0;
                uint64_t v = 0;
                ok = d.GetU64(&t) && d.GetU64(&v) && d.ok();
                if (ok)
                {
                    term_ = t;
                    voted_for_ = v;
                }
            }
            else if (type == kEntry)
            {
                LogEntry entry;
                uint8_t etype = 0;
                ok = d.GetU64(&entry.index) && d.GetU64(&entry.term) && d.GetU8(&etype) &&
                     d.GetString(&entry.data) && d.ok();
                if (ok)
                {
                    entry.type = static_cast<EntryType>(etype);

                    const uint64_t expected = entries_.empty() ? 1 : entries_.back().index + 1;
                    if (entry.index != expected)
                    {
                        LOG_WARN("wal %s: non-contiguous index %llu (expected %llu)", path_.c_str(),
                                 static_cast<unsigned long long>(entry.index),
                                 static_cast<unsigned long long>(expected));
                        ok = false;
                    }
                    else
                    {
                        entries_.push_back(std::move(entry));
                    }
                }
            }
            else if (type == kTruncate)
            {
                uint64_t from = 0;
                ok = d.GetU64(&from) && d.ok();
                if (ok)
                {
                    while (!entries_.empty() && entries_.back().index >= from)
                        entries_.pop_back();
                    if (from > 0 && commit_index_ >= from)
                        commit_index_ = from - 1;
                }
            }
            else if (type == kCommit)
            {
                uint64_t c = 0;
                ok = d.GetU64(&c) && d.ok();
                if (ok)
                {
                    commit_index_ = c;
                }
            }
            else
            {
                ok = false;
            }
            if (!ok)
            {
                if (!at_tail(len))
                {
                    *err = "wal corrupted in the middle at offset " + std::to_string(offset) + " (file size " +
                           std::to_string(file_size) + ", last good offset " + std::to_string(good_offset) +
                           "): bad record payload，拒绝启动（请从其它副本重建该节点，或确认后删除其 data 目录）";
                    LOG_ERROR("%s", err->c_str());
                    return false;
                }
                bad_tail("bad record payload", len);
                break;
            }
            offset += 8 + len;
            good_offset = offset;
        }

        if (::ftruncate(fd_, static_cast<off_t>(good_offset)) != 0)
        {
            *err = "wal truncate failed: " + ErrnoString();
            return false;
        }
        end_offset_ = good_offset;
        LOG_INFO("wal %s loaded: %zu entries, last_index=%llu term=%llu voted_for=%llu", path_.c_str(),
                 entries_.size(), static_cast<unsigned long long>(last_index()),
                 static_cast<unsigned long long>(term_), static_cast<unsigned long long>(voted_for_));
        return true;
    }

    void Wal::Close()
    {
        if (fd_ >= 0)
        {
            ::close(fd_);
            fd_ = -1;
        }
    }

    size_t Wal::file_size() const
    {
        struct stat st{};
        if (fd_ < 0 || ::fstat(fd_, &st) != 0)
            return 0;
        return static_cast<size_t>(st.st_size);
    }

    void SetReadFaultForTesting(int countdown)
    {
        g_read_fault.store(countdown < 0 ? 0 : countdown);
    }

} 
