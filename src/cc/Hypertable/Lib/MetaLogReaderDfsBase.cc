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
#include "Common/Checksum.h"
#include "Common/Serialization.h"
#include "Filesystem.h"
#include "MetaLogVersion.h"
#include "MetaLogReaderDfsBase.h"

using namespace Hypertable;
using namespace Serialization;

namespace {
  const uint32_t READAHEAD_BUFSZ = 1024 * 1024;
  const uint32_t OUTSTANDING_READS = 1;
}

MetaLogReaderDfsBase::MetaLogReaderDfsBase(Filesystem *fs, const String &path) :
    m_fs(fs), m_path(path), m_cur(0) {
  if (fs) {
    m_file_size = fs->length(path);
    m_fd = fs->open_buffered(path, READAHEAD_BUFSZ, OUTSTANDING_READS);
  }
}

MetaLogReader::ScanEntry *
MetaLogReaderDfsBase::next(ScanEntry &entry) {
  if (m_cur >= m_file_size)
    return 0;

  try {
    uint8_t buf[ML_ENTRY_HEADER_SIZE];
    const uint8_t *p = buf;
    size_t nread = m_fs->read(m_fd, buf, ML_ENTRY_HEADER_SIZE);
    m_cur += nread;

    if (nread != ML_ENTRY_HEADER_SIZE)
      HT_THROW(Error::METALOG_ENTRY_TRUNCATED, "reading header");

    HT_TRY("decoding scan entry",
      entry.checksum = decode_i32(&p, &nread);
      entry.timestamp = decode_i64(&p, &nread);
      entry.type = decode_i8(&p, &nread);
      entry.payload_size = decode_i32(&p, &nread));

    entry.buf.clear();
    entry.buf.reserve(entry.payload_size);
    nread = m_fs->read(m_fd, entry.buf.ptr, entry.payload_size);
    m_cur += nread;
    entry.buf.ptr += nread;

    HT_DEBUG_OUT <<"checksum="<< entry.checksum <<" timestamp="
        << entry.timestamp <<" type="<< entry.type <<" payload="
        << entry.payload_size << HT_END;

    if (nread != entry.payload_size)
      HT_THROW(Error::METALOG_ENTRY_TRUNCATED, "reading payload");

    // checksum include timestamp, type, payload size and payload data
    uint32_t checksum = crc32(buf + 4, ML_ENTRY_HEADER_SIZE - 4);
    checksum = crc32_update(checksum, entry.buf.base, entry.payload_size);

    if (entry.checksum != checksum)
      HT_THROWF(Error::METALOG_CHECKSUM_MISMATCH, "type=%d expected %x, got %x",
                entry.type, entry.checksum, checksum);

    return &entry;
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error reading metalog: %s: read %lu/%lu",
               m_path.c_str(), m_cur, m_file_size);
  }
}
