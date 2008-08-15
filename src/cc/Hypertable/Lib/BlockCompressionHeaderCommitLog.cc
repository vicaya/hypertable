/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include "Common/Error.h"

#include "Common/Serialization.h"

#include "BlockCompressionHeaderCommitLog.h"

using namespace Hypertable;
using namespace Serialization;

const size_t BlockCompressionHeaderCommitLog::LENGTH;

BlockCompressionHeaderCommitLog::BlockCompressionHeaderCommitLog()
  : BlockCompressionHeader(), m_revision(0) {
}

BlockCompressionHeaderCommitLog::BlockCompressionHeaderCommitLog(
    const char *magic, int64_t revision)
  : BlockCompressionHeader(magic), m_revision(revision) {
}

void BlockCompressionHeaderCommitLog::encode(uint8_t **bufp) {
  uint8_t *base = *bufp;
  BlockCompressionHeader::encode(bufp);
  encode_i64(bufp, m_revision);
  if ((size_t)(*bufp - base) + 2 == length())
    write_header_checksum(base, bufp);
}


void BlockCompressionHeaderCommitLog::decode(const uint8_t **bufp,
                                             size_t *remainp) {
  const uint8_t *base = *bufp;

  BlockCompressionHeader::decode(bufp, remainp);
  m_revision = decode_i64(bufp, remainp);

  if ((size_t)(*bufp - base) == length() - 2) {
    *bufp += 2;
    *remainp -= 2;
  }
}
