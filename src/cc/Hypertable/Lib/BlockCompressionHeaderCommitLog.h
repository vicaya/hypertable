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

#include "Common/DynamicBuffer.h"

#include "BlockCompressionHeader.h"
#include "Types.h"

namespace Hypertable {

  /**
   * Base class for compressed block header for Cell Store blocks.
   */
  class BlockCompressionHeaderCommitLog : public BlockCompressionHeader {
  public:
    BlockCompressionHeaderCommitLog();
    BlockCompressionHeaderCommitLog(const char magic[10], uint64_t timestamp, TableIdentifier *table);

    void set_timestamp(uint64_t timestamp) { m_timestamp = timestamp; }
    uint64_t get_timestamp() { return m_timestamp; }

    void get_table_identifier(TableIdentifier &table_id) {
      memcpy(&table_id, &m_table, sizeof(table_id));
    }

    virtual size_t fixed_length() { return 34; }
    virtual size_t encoded_length() { return fixed_length() + EncodedLengthTableIdentifier(m_table); }
    virtual void   encode(uint8_t **buf_ptr);
    virtual int    decode_fixed(uint8_t **buf_ptr, size_t *remaining_ptr);
    virtual int    decode_variable(uint8_t **buf_ptr, size_t *remaining_ptr);
    
  private:
    uint64_t m_timestamp;
    TableIdentifier m_table;
    DynamicBuffer m_buffer;
  };

}

#endif // HYPERTABLE_BLOCKCOMPRESSIONHEADERCOMMITLOG_H
