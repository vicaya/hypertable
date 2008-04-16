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
    BlockCompressionHeaderCommitLog(const char *magic, uint64_t timestamp);

    void set_timestamp(uint64_t timestamp) { m_timestamp = timestamp; }
    uint64_t get_timestamp() { return m_timestamp; }

    virtual size_t length() { return LENGTH; }
    virtual void   encode(uint8_t **buf_ptr);
    virtual int    decode(uint8_t **buf_ptr, size_t *remaining_ptr);
    
  private:
    uint64_t m_timestamp;
  };

}

#endif // HYPERTABLE_BLOCKCOMPRESSIONHEADERCOMMITLOG_H
