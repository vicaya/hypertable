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
#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Serialization.h"

#include "Hypertable/Lib/KeySpec.h"

#include "SerializedCellsWriter.h"
#include "SerializedCellsFlag.h"

using namespace Hypertable;


bool SerializedCellsWriter::add(const char *row, const char *column_family,
                                const char *column_qualifier, int64_t timestamp,
                                const void *value, int32_t value_length) {
  int32_t row_length = strlen(row);
  int32_t column_family_length = strlen(column_family);
  int32_t column_qualifier_length = strlen(column_qualifier);
  int32_t length = 9 + row_length + column_family_length + column_qualifier_length + value_length; 
  uint8_t flag = 0;

  if (timestamp == AUTO_ASSIGN)
    flag |= SerializedCellsFlag::AUTO_TIMESTAMP;
  else if (timestamp != TIMESTAMP_NULL) {
    flag |= SerializedCellsFlag::HAVE_TIMESTAMP;
    length += 8;
  }

  // need to leave room for the termination byte
  if (length > (int32_t)m_buf.remaining()) {
    if (m_grow)
      m_buf.ensure(length);
    else {
      if (!m_buf.empty())
        return false;
      m_buf.grow(length);
    }
  }

  // flag byte
  *m_buf.ptr++ = flag;

  // timestamp
  if ((flag & SerializedCellsFlag::HAVE_TIMESTAMP) != 0)
    Serialization::encode_i64(&m_buf.ptr, timestamp);

  // revision
  if ((flag & SerializedCellsFlag::HAVE_REVISION) &&
      (flag & SerializedCellsFlag::REV_IS_TS) == 0)
    Serialization::encode_i64(&m_buf.ptr, 0);

  // row
  memcpy(m_buf.ptr, row, row_length);
  m_buf.ptr += row_length;
  *m_buf.ptr++ = 0;

  // column_family
  memcpy(m_buf.ptr, column_family, column_family_length);
  m_buf.ptr += column_family_length;
  *m_buf.ptr++ = 0;

  // column_qualifier
  memcpy(m_buf.ptr, column_qualifier, column_qualifier_length);
  m_buf.ptr += column_qualifier_length;
  *m_buf.ptr++ = 0;

  Serialization::encode_i32(&m_buf.ptr, value_length);
  memcpy(m_buf.ptr, value, value_length);
  m_buf.ptr += value_length;

  return true;
}
