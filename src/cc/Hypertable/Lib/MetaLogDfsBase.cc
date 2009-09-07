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
#include "Common/StringExt.h"
#include "Filesystem.h"
#include "MetaLogDfsBase.h"
#include "MetaLogVersion.h"

#include <vector>

using namespace Hypertable;
using namespace Serialization;
using namespace std;

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

MetaLogDfsBase::MetaLogDfsBase(Filesystem *fs, const String &path)
  : m_fd(-1), m_fs(fs), m_path(path), m_fileno(-1) {
  if (fs) {
    get_filename();
    if (m_fileno == -1) {
      m_filename = path + "/" + "0";
      m_fd = create(m_filename);
      m_fileno = 0;
    }
  }
}

void MetaLogDfsBase::get_filename() {
  vector<int32_t> fileno_vec;
  m_fileno = -1;
  int32_t num;
  std::vector<String> listing;
  m_fs->readdir(m_path, listing);
  for (size_t i=0; i<listing.size(); i++) {
    num = atoi(listing[i].c_str());
    fileno_vec.push_back(num);
    if (num > m_fileno)
      m_fileno = num;
  }
  if (m_fileno == -1)
    m_filename = "";
  else {
    m_filename = m_path;
    m_filename += String("/") + m_fileno;
  }
  // remove all but the last 10
  if (fileno_vec.size() > 10) {
    String tmp_name;
    make_heap(fileno_vec.begin(), fileno_vec.end(), reverse_sort());
    sort_heap(fileno_vec.begin(), fileno_vec.end(), reverse_sort());
    for (size_t i=10; i<fileno_vec.size(); i++) {
      tmp_name = m_path;
      tmp_name += String("/") + fileno_vec[i];
      m_fs->remove(tmp_name);
    }
  }
}

int
MetaLogDfsBase::create(const String &path, bool overwrite) {
  HT_DEBUG_OUT <<"path="<< path <<" overwrite="<< overwrite <<" buffer size="
      << DFS_BUFFER_SIZE <<" replicas="<< DFS_NUM_REPLICAS <<" block size="
      << DFS_BLOCK_SIZE << HT_END;
  return m_fs->create(path, overwrite, DFS_BUFFER_SIZE, DFS_NUM_REPLICAS,
                      DFS_BLOCK_SIZE);
}

void
MetaLogDfsBase::close() {
  try {
    m_fs->close(m_fd);
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
  uint8_t *base = buf.ptr;
  buf.ptr += ML_ENTRY_HEADER_SIZE;    // reserve header space
  entry->write(buf);                  // serialize entry to buffer
  // fill out the entry header
  uint8_t *p = base + 4;          // reserve 32-bit checksum space
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
  m_fs->append(m_fd, sbuf, Filesystem::O_FLUSH);
}
