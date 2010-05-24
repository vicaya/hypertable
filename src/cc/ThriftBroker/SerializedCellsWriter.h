/**
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef HYPERTABLE_SERIALIZEDCELLSWRITER_H
#define HYPERTABLE_SERIALIZEDCELLSWRITER_H

#include "Common/DynamicBuffer.h"

#include "Hypertable/Lib/Cell.h"
#include "Hypertable/Lib/KeySpec.h"

#include "SerializedCellsFlag.h"

namespace Hypertable {

  class SerializedCellsWriter {
  public:
  
    SerializedCellsWriter(int32_t size, bool grow=false) :
      m_buf(size), m_finalized(false), m_grow(grow) { }

    bool add(Cell &cell) {
      return add(cell.row_key, cell.column_family, cell.column_qualifier,
                 cell.timestamp, cell.value, cell.value_len);
    }

    bool add(const char *row, const char *column_family,
             const char *column_qualifier, int64_t timestamp,
             const void *value, int32_t value_length);

    void finalize(uint8_t flag) {
      if (m_grow)
        m_buf.ensure(1);
      *m_buf.ptr++ = SerializedCellsFlag::EOB | flag;
      m_finalized = true;
    }

    uint8_t *get_buffer() { return m_buf.base; }
    int32_t get_buffer_length() { return m_buf.fill(); }

    void get_buffer(const uint8_t **bufp, int32_t *lenp) {
      if (!m_finalized)
        finalize(SerializedCellsFlag::EOB);
      *bufp = m_buf.base;
      *lenp = m_buf.fill();
    }

    bool empty() { return m_buf.empty(); }    
    
    void clear() { m_buf.clear(); }

  private:
    DynamicBuffer m_buf;
    bool m_finalized;
    bool m_grow;
  };

}

#endif // HYPERTABLE_SERIALIZEDCELLSWRITER_H
