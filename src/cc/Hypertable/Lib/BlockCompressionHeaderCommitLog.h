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
    BlockCompressionHeaderCommitLog(const char magic[10], uint64_t timestamp, const char *tablename);

    void set_timestamp(uint64_t timestamp) { m_timestamp = timestamp; }
    uint64_t get_timestamp() { return m_timestamp; }

    void set_tablename(const char *tablename) { m_tablename = tablename; }
    const char *get_tablename() { return m_tablename; }

    virtual size_t fixed_length() { return 34; }
    virtual size_t encoded_length() { return fixed_length() + (m_tablename ? strlen(m_tablename) : 0) + 1; }
    virtual void   encode(uint8_t **buf_ptr);
    virtual int    decode_fixed(uint8_t **buf_ptr, size_t *remaining_ptr);
    virtual int    decode_variable(uint8_t **buf_ptr, size_t *remaining_ptr);
    
  private:
    uint64_t m_timestamp;
    const char *m_tablename;
  };

}

#endif // HYPERTABLE_BLOCKCOMPRESSIONHEADERCOMMITLOG_H

