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

#include "SerializedCellsReader.h"
#include "SerializedCellsFlag.h"

using namespace Hypertable;

bool SerializedCellsReader::next() {
  size_t remaining = m_end - m_ptr;

  if (m_eob)
    return false;

  if (remaining == 0)
    HT_THROW(Error::SERIALIZATION_INPUT_OVERRUN, "");

  m_flag = Serialization::decode_i8(&m_ptr, &remaining);

  if (m_flag & SerializedCellsFlag::EOB) {
    m_eob = true;
    return false;
  }

  if (m_flag & SerializedCellsFlag::HAVE_TIMESTAMP)
    m_timestamp = Serialization::decode_i64(&m_ptr, &remaining);

  if ((m_flag & SerializedCellsFlag::HAVE_REVISION) &&
      (m_flag & SerializedCellsFlag::REV_IS_TS) == 0)
    m_revision = Serialization::decode_i64(&m_ptr, &remaining);

  // row
  m_row = (const char *)m_ptr;
  HT_ASSERT(*m_row);
  while (*m_ptr && m_ptr<m_end)
    m_ptr++;
  if (m_ptr == m_end)
    HT_THROW(Error::SERIALIZATION_INPUT_OVERRUN, "");    
  m_ptr++;

  // column_family
  m_column_family = (const char *)m_ptr;
  while (*m_ptr && m_ptr<m_end)
    m_ptr++;
  if (m_ptr == m_end)
    HT_THROW(Error::SERIALIZATION_INPUT_OVERRUN, "");    
  m_ptr++;

  // column_qualifier
  m_column_qualifier = (const char *)m_ptr;
  while (*m_ptr && m_ptr<m_end)
    m_ptr++;
  if (m_ptr == m_end)
    HT_THROW(Error::SERIALIZATION_INPUT_OVERRUN, "");    
  m_ptr++;

  remaining = m_end - m_ptr;
  m_value_len = Serialization::decode_i32(&m_ptr, &remaining);

  if (m_value_len > remaining)
    HT_THROW(Error::SERIALIZATION_INPUT_OVERRUN, "");

  m_value = m_ptr;
  m_ptr += m_value_len;

  m_cell_flag = *m_ptr++;

  if( m_cell_flag == FLAG_DELETE_ROW && !*m_column_family )
    m_column_family = 0;
    
  return true;
}
