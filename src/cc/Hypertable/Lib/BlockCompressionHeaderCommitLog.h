/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_BLOCKCOMPRESSIONHEADERCOMMITLOG_H
#define HYPERTABLE_BLOCKCOMPRESSIONHEADERCOMMITLOG_H

#include "Hypertable/Lib/BlockCompressionHeader.h"

namespace Hypertable {

  /**
   * Base class for compressed block header for Cell Store blocks.
   */
  class BlockCompressionHeaderCommitLog : public BlockCompressionHeader {
  public:
    BlockCompressionHeaderCommitLog();
    BlockCompressionHeaderCommitLog(const char magic[12], uint64_t timestamp, const char *tablename);

    void set_timestamp(uint64_t timestamp) { m_timestamp = timestamp; }
    void set_tablename(const char *tablename) { m_tablename = tablename; }

    virtual size_t encoded_length() { return 33 + (m_tablename ? strlen(m_tablename) : 0); }
    virtual void   encode(uint8_t **buf_ptr);
    virtual int    decode(uint8_t **buf_ptr, size_t *remaining_ptr);
    
  private:
    uint64_t m_timestamp;
    const char *m_tablename;
  };

}

#endif // HYPERTABLE_BLOCKCOMPRESSIONHEADERCOMMITLOG_H

