/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#include "Common/Error.h"

#include "AsyncComm/Serialization.h"

#include "BlockCompressionHeaderCommitLog.h"

using namespace Hypertable;

const size_t BlockCompressionHeaderCommitLog::LENGTH;

BlockCompressionHeaderCommitLog::BlockCompressionHeaderCommitLog() : BlockCompressionHeader(), m_timestamp(0) {
}

BlockCompressionHeaderCommitLog::BlockCompressionHeaderCommitLog(const char *magic, uint64_t timestamp) : BlockCompressionHeader(magic), m_timestamp(timestamp) {
}

void BlockCompressionHeaderCommitLog::encode(uint8_t **buf_ptr) {
  uint8_t *base = *buf_ptr;
  BlockCompressionHeader::encode(buf_ptr);
  Serialization::encode_long(buf_ptr, m_timestamp);
  if ((size_t)(*buf_ptr - base) + 2 == length()) 
    write_header_checksum(base, buf_ptr);
}


int BlockCompressionHeaderCommitLog::decode(uint8_t **buf_ptr, size_t *remaining_ptr) {
  uint8_t *base = *buf_ptr;
  int error;

  if ((error = BlockCompressionHeader::decode(buf_ptr, remaining_ptr)) != Error::OK)
    return error;

  if (!Serialization::decode_long(buf_ptr, remaining_ptr, &m_timestamp))
    return Error::BLOCK_COMPRESSOR_TRUNCATED;

  if ((size_t)(*buf_ptr - base) == length() - 2) {
    *buf_ptr += 2;
    *remaining_ptr -= 2;
  }
  
  return Error::OK;
}
