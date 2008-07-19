/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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
  const uint32_t DFS_BUFFER_SIZE = -1;  // use dfs defaults
  const uint32_t DFS_NUM_REPLICAS = -1; // ditto
  const int64_t DFS_BLOCK_SIZE = -1;   // ditto
  const size_t ML_ENTRY_BUFFER_RESERVE = ML_ENTRY_HEADER_SIZE + 512;
  const size_t DFS_XFER_SIZE = 32768;
} // local namespace

MetaLogDfsBase::MetaLogDfsBase(Filesystem *fs, const String &path) :
    m_fs(fs), m_path(path) {
  if (fs) {
    if (fs->exists(path)) {
      String tmp_path = path + ".tmp";
      int tmp_fd;
      size_t toread, nread, nwritten;
      uint8_t *buf = 0;
      try {
	m_fs->rename(path, tmp_path);
	tmp_fd = m_fs->open(tmp_path);
	m_fd = m_fs->create(path, false, DFS_BUFFER_SIZE, DFS_NUM_REPLICAS,
			    DFS_BLOCK_SIZE);
	toread = nread = DFS_XFER_SIZE;
	while (nread == DFS_XFER_SIZE) {
	  buf = new uint8_t [ DFS_XFER_SIZE ];
	  nread = m_fs->read(tmp_fd, buf, DFS_XFER_SIZE);
	  if (nread > 0) {
	    StaticBuffer sbuf(buf, nread);
	    nwritten = m_fs->append(m_fd, sbuf, 0);
	    if (nwritten != nread) {
	      delete buf;
	      buf = 0;
	      HT_THROW(Error::DFSBROKER_IO_ERROR, "problem making copying of metalog");
	    }
	  }
	}
	buf = 0;
	m_fs->close(tmp_fd);
	m_fs->remove(tmp_path);
      }
      catch (Exception &e) {
	delete [] buf;
	HT_THROW2F(e.code(), e, "Error opening metalog: %s", path.c_str());
      }
    }
    else 
      m_fd = create(path);
  }
}

int
MetaLogDfsBase::create(const String &path, bool overwrite) {
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
  HT_DEBUG_OUT <<"checksum="<< checksum <<" timestamp="<< entry->timestamp
      <<" type="<< entry->get_type() <<" payload="<< payload_size << HT_END;
}

void
MetaLogDfsBase::write_unlocked(DynamicBuffer &buf) {
  StaticBuffer sbuf(buf);
  m_fs->append(m_fd, sbuf, Filesystem::O_FLUSH);
}
