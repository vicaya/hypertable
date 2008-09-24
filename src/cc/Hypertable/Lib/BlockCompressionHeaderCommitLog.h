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

#ifndef HYPERTABLE_BLOCKCOMPRESSIONHEADERCOMMITLOG_H
#define HYPERTABLE_BLOCKCOMPRESSIONHEADERCOMMITLOG_H

#include "BlockCompressionHeader.h"

namespace Hypertable {

  /**
   * Base class for compressed block header for Cell Store blocks.
   */
  class BlockCompressionHeaderCommitLog : public BlockCompressionHeader {

  public:

    static const size_t LENGTH = BlockCompressionHeader::LENGTH + 8;

    BlockCompressionHeaderCommitLog();
    BlockCompressionHeaderCommitLog(const char *magic, int64_t revision);

    void set_revision(int64_t revision) { m_revision = revision; }
    int64_t get_revision() { return m_revision; }

    virtual size_t length() { return LENGTH; }
    virtual void   encode(uint8_t **bufp);
    virtual void   decode(const uint8_t **bufp, size_t *remainp);

  private:
    int64_t m_revision;
  };

}

#endif // HYPERTABLE_BLOCKCOMPRESSIONHEADERCOMMITLOG_H
