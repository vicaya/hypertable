/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include "Common/Logger.h"
#include "Common/Serialization.h"
#include "Common/Checksum.h"
#include "Common/FileUtils.h"
#include "Common/StringExt.h"
#include "Common/System.h"
#include "Common/Filesystem.h"
#include "Common/Path.h"
#include "Common/Config.h"
#include "MetaLogDfsBase.h"
#include "MetaLogVersion.h"

#include <cctype>
#include <vector>

using namespace Hypertable;
using namespace Serialization;
using namespace std;
using namespace Hypertable::OldMetaLog;

namespace {
  const int32_t DFS_BUFFER_SIZE = -1;  // use dfs defaults
  const int32_t DFS_NUM_REPLICAS = -1; // ditto
  const int64_t DFS_BLOCK_SIZE = -1;   // ditto
  const size_t ML_ENTRY_BUFFER_RESERVE = ML_ENTRY_HEADER_SIZE + 512;
  const size_t DFS_XFER_SIZE = 32768;
  struct reverse_sort {
    bool operator()(const int32_t i1, const int32_t i2) const {
      return i1 > i2;
    }
  };
} // local namespace

MetaLogDfsBase::MetaLogDfsBase(Filesystem *fs, const String &path, const String &backup_dir)
  : m_fd(-1), m_fs(fs), m_path(path), m_fileno(-1) {
  HT_EXPECT(Config::properties, Error::FAILED_EXPECTATION);
  Path data_dir = Config::properties->get_str("Hypertable.DataDirectory"); 
  m_backup_path = (data_dir /= "/run/" + backup_dir).string(); 
  if (!FileUtils::exists(m_backup_path))
    FileUtils::mkdirs(m_backup_path);
  find_or_create_file();
}

void MetaLogDfsBase::find_or_create_file() {
  if (m_fs) {
    get_filename();
    if (m_fileno == -1) {
      m_filename = m_path + "/0";
      m_fd = create(m_filename);
      m_fileno = 0;
      m_backup_filename = m_backup_path + "/0";
      m_backup_fd = create_backup(m_backup_filename);
    }
  }
}

void MetaLogDfsBase::get_filename() {
  const char *ptr;
  vector<int32_t> fileno_vec;
  m_fileno = -1;
  int32_t num;
  std::vector<String> listing;
  m_fs->readdir(m_path, listing);
  for (size_t i=0; i<listing.size(); i++) {
    for (ptr=listing[i].c_str(); ptr; ++ptr) {
      if (!isdigit(*ptr))
        break;
    }
    if (*ptr != 0) {
      HT_WARNF("Invalid META LOG file name encountered '%s', skipping...",
               listing[i].c_str());
      continue;
    }
    num = atoi(listing[i].c_str());
    fileno_vec.push_back(num);
    if (num > m_fileno)
      m_fileno = num;
  }
  if (m_fileno == -1) {
    m_filename = "";
    m_backup_filename = "";
  }
  else {
    m_filename = m_path;
    m_filename += String("/") + m_fileno;
    m_backup_filename = m_backup_path;
    m_backup_filename += String("/") + m_fileno;
  }
  // remove all but the last 10
  if (fileno_vec.size() > 10) {
    String tmp_name;
    make_heap(fileno_vec.begin(), fileno_vec.end(), reverse_sort());
    sort_heap(fileno_vec.begin(), fileno_vec.end(), reverse_sort());
    for (size_t i=10; i<fileno_vec.size(); i++) {
      // remove from DFS
      tmp_name = m_path;
      tmp_name += String("/") + fileno_vec[i];
      m_fs->remove(tmp_name);
      // remove local backup
      tmp_name = m_backup_path;
      tmp_name += String("/") + fileno_vec[i];
      if (FileUtils::exists(tmp_name))
	FileUtils::unlink(tmp_name);
    }
  }
}

int
MetaLogDfsBase::create(const String &path, bool overwrite) {
  HT_DEBUG_OUT <<"path="<< path <<" overwrite="<< overwrite <<" buffer size="
      << DFS_BUFFER_SIZE <<" replicas="<< DFS_NUM_REPLICAS <<" block size="
      << DFS_BLOCK_SIZE << HT_END;
  return m_fs->create(path, overwrite ? Filesystem::OPEN_FLAG_OVERWRITE : 0,
                      DFS_BUFFER_SIZE, DFS_NUM_REPLICAS, DFS_BLOCK_SIZE);
}

int
MetaLogDfsBase::create_backup(const String &path) {
  HT_DEBUG_OUT <<"path="<< path <<" buffer size="
      << DFS_BUFFER_SIZE <<" replicas="<< DFS_NUM_REPLICAS <<" block size="
      << DFS_BLOCK_SIZE << HT_END;
  return ::open(path.c_str(), O_CREAT|O_TRUNC|O_WRONLY, 0644);
}

void
MetaLogDfsBase::close() {
  try {
    if (m_fd != -1) {
      m_fs->close(m_fd, (DispatchHandler *)0);
      m_fd = -1;
      ::close(m_backup_fd);
      m_backup_fd = -1;
    }
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error closing metalog: %s (fd=%d)", m_filename.c_str(), m_fd);
  }
}

void
MetaLogDfsBase::write(MetaLogEntry *entry) {
  ScopedLock lock(m_mutex);
  m_buf.clear();
  m_buf.reserve(ML_ENTRY_BUFFER_RESERVE);
  serialize_entry(entry, m_buf);
  // write to the dfs
  write_unlocked(m_buf);
}

void
MetaLogDfsBase::serialize_entry(MetaLogEntry *entry, DynamicBuffer &buf) {
  size_t base_offset = buf.fill();
  buf.ptr += ML_ENTRY_HEADER_SIZE;    // reserve header space
  entry->write(buf);                  // serialize entry to buffer
  // fill out the entry header
  uint8_t *base = buf.base + base_offset;
  uint8_t *p = base + 4;              // reserve 32-bit checksum space
  encode_i64(&p, entry->timestamp);
  *p++ = entry->get_type();
  size_t total_size = buf.ptr - base;
  size_t payload_size = total_size - ML_ENTRY_HEADER_SIZE;
  encode_i32(&p, payload_size);
  p = base;
  uint32_t checksum = crc32(base + 4, total_size - 4);
  encode_i32(&p, checksum);
  HT_DEBUG_OUT << entry <<" payload="<< payload_size << HT_END;
}


void
MetaLogDfsBase::write_unlocked(DynamicBuffer &buf) {
  StaticBuffer sbuf(buf);
  FileUtils::write(m_backup_fd, sbuf.base, sbuf.size);
  m_fs->append(m_fd, sbuf, Filesystem::O_FLUSH);
}
