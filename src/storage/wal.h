#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/log_entry.h"

namespace raftkv {

class Wal {
 public:
  enum RecordType : uint8_t { kMeta = 1, kEntry = 2, kTruncate = 3, kCommit = 4 };

  Wal() = default;
  ~Wal();

  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;


  bool Open(const std::string& path, bool sync, std::string* err);
  bool Load(std::string* err);
  void Close();


  bool AppendMeta(uint64_t term, uint64_t voted_for, std::string* err);
  bool AppendEntry(const LogEntry& entry, std::string* err);
  bool AppendTruncate(uint64_t from_index, std::string* err);
  bool AppendCommit(uint64_t commit_index, std::string* err);

  uint64_t term() const { return term_; }
  uint64_t voted_for() const { return voted_for_; }
  uint64_t commit_index() const { return commit_index_; }
  const std::vector<LogEntry>& entries() const { return entries_; }
  uint64_t last_index() const { return entries_.empty() ? 0 : entries_.back().index; }
  uint64_t last_term() const { return entries_.empty() ? 0 : entries_.back().term; }


  size_t file_size() const;

  bool ok() const { return !failed_; }
  const std::string& failure_reason() const { return failure_reason_; }

  uint64_t end_offset() const { return end_offset_; }

 private:
  bool AppendRecord(uint8_t type, const std::string& payload, bool force_sync, std::string* err);
  bool WriteHeader(std::string* err);

  int fd_ = -1;
  bool sync_ = false;
  std::string path_;
  uint64_t term_ = 0;
  uint64_t voted_for_ = 0;
  uint64_t commit_index_ = 0;
  uint64_t end_offset_ = 0;
  bool failed_ = false;
  std::string failure_reason_;
  std::vector<LogEntry> entries_;
};


void SetReadFaultForTesting(int countdown);

}  
