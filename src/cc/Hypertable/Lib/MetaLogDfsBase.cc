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
#include "Filesystem.h"
#include "MetaLogDfsBase.h"
#include "MetaLogVersion.h"

using namespace Hypertable;
using namespace Serialization;

namespace {
  const int32_t DFS_BUFFER_SIZE = -1;  // use dfs defaults
  const int32_t DFS_NUM_REPLICAS = -1; // ditto
  const int64_t DFS_BLOCK_SIZE = -1;   // ditto
  const size_t ML_ENTRY_BUFFER_RESERVE = ML_ENTRY_HEADER_SIZE + 512;
  const size_t DFS_XFER_SIZE = 32768;
} // local namespace

MetaLogDfsBase::MetaLogDfsBase(Filesystem *fs, const String &path)
  : m_fd(-1), m_fs(fs), m_path(path) {
  if (fs && !fs->exists(path))
    m_fd = create(path);
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
    HT_THROW2F(e.code(), e, "Error closing metalog: %s", m_path.c_str());
  }
}

void
MetaLogDfsBase::write(MetaLogEntry *entry) {
  ScopedLock lock(m_mutex);
  m_buf.reserve(ML_ENTRY_BUFFER_RESERVE);
  m_buf.ptr += ML_ENTRY_HEADER_SIZE;    // reserve header space
  entry->write(m_buf);                  // serialize entry to buffer
  // fill out the entry header
  uint8_t *p = m_buf.base + 4;          // reserver 32-bit checksum space
  encode_i64(&p, entry->timestamp);
  *p++ = entry->get_type();
  size_t payload_size = m_buf.fill() - ML_ENTRY_HEADER_SIZE;
  encode_i32(&p, payload_size);
  p = m_buf.base;
  uint32_t checksum = crc32(m_buf.base + 4, m_buf.fill() - 4);
  encode_i32(&p, checksum);
  // write to the dfs
  write_unlocked(m_buf);
  HT_DEBUG_OUT << entry <<" payload="<< payload_size << HT_END;
}

void
MetaLogDfsBase::write_unlocked(DynamicBuffer &buf) {
  StaticBuffer sbuf(buf);
  m_fs->append(m_fd, sbuf, Filesystem::O_FLUSH);
}
