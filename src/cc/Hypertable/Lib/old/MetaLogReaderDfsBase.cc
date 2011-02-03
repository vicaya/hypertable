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
#include "Common/Checksum.h"
#include "Common/Serialization.h"
#include "Common/StringExt.h"
#include "Common/Filesystem.h"
#include "MetaLogVersion.h"
#include "MetaLogReaderDfsBase.h"

using namespace Hypertable;
using namespace Hypertable::OldMetaLog;
using namespace Serialization;

namespace {
  const uint32_t READAHEAD_BUFSZ = 1024 * 1024;
  const uint32_t OUTSTANDING_READS = 1;
}

MetaLogReaderDfsBase::MetaLogReaderDfsBase(Filesystem *fs, const String &path)
  : MetaLogReader(), m_fd(-1), m_fs(fs), m_path(path), m_cur(0) {

  if (fs) {
    get_filename();
    if (m_filename != "") {
      m_file_size = fs->length(m_filename);
      m_fd = fs->open_buffered(m_filename, 0, READAHEAD_BUFSZ, OUTSTANDING_READS);
    }
  }
}


MetaLogReaderDfsBase::~MetaLogReaderDfsBase() {
  if (m_fs && m_fd != -1) {
    try {
      m_fs->close(m_fd);
      m_fd = -1;
    }
    catch (Exception &e) {
      HT_ERRORF("Problem closing MetaLog '%s' - %s - %s",
                m_filename.c_str(), Error::get_text(e.code()), e.what());
    }
  }
}

void MetaLogReaderDfsBase::get_filename() {
  const char *ptr;
  int32_t fileno = -1;
  int32_t num;
  std::vector<String> listing;
  m_fs->readdir(m_path, listing);
  for (size_t i=0; i<listing.size(); i++) {
    for (ptr=listing[i].c_str(); ptr; ++ptr) {
      if (!isdigit(*ptr))
        break;
    }
    if (*ptr != 0) {
      HT_WARNF("Invalid RSML file name encountered '%s', skipping...",
               listing[i].c_str());
      continue;
    }
    num = atoi(listing[i].c_str());
    if (num > fileno)
      fileno = num;
  }
  if (fileno != -1) {
    m_filename = m_path;
    m_filename += String("/") + fileno;
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
    if (entry.payload_size > 0) {
      entry.buf.reserve(entry.payload_size);
      nread = m_fs->read(m_fd, entry.buf.ptr, entry.payload_size);
      m_cur += nread;
      entry.buf.ptr += nread;
    }
    else
      nread = 0;

    HT_DEBUG_OUT <<"checksum="<< entry.checksum <<" timestamp="
        << entry.timestamp <<" type="<< entry.type <<" payload="
        << entry.payload_size << HT_END;

    if (nread != entry.payload_size)
      HT_THROW(Error::METALOG_ENTRY_TRUNCATED, "reading payload");

    // checksum include timestamp, type, payload size and payload data
    uint32_t checksum = crc32(buf + 4, ML_ENTRY_HEADER_SIZE - 4);
    if (entry.payload_size > 0)
      checksum = crc32_update(checksum, entry.buf.base, entry.payload_size);

    if (entry.checksum != checksum)
      HT_THROWF(Error::METALOG_CHECKSUM_MISMATCH, "type=%d expected %x, got %x",
                entry.type, entry.checksum, checksum);

    return &entry;
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error reading metalog: %s: read %llu/%llu",
               m_path.c_str(), (Llu)m_cur, (Llu)m_file_size);
  }
}
