/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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

#include "Common/Compat.h"
#include "Common/Serialization.h"

#include "KeyDecompressorPrefix.h"

using namespace Hypertable;


void KeyDecompressorPrefix::reset() {
  m_bufs[0].clear();
  m_bufs[1].clear();
  m_current_base = 0;
  m_serialized_key.ptr = 0;
  m_first = false;
}

const uint8_t *KeyDecompressorPrefix::add(const uint8_t *next_base) {
  int current = m_first ? 0 : 1;
  int next = m_first ? 1 : 0;
  const uint8_t *next_ptr;
  SerializedKey serkey(next_base);
  size_t next_length = serkey.decode_length(&next_ptr);
  uint8_t control = *next_ptr++;
  size_t remaining = next_length - 1;
  uint32_t matching = Serialization::decode_vi32(&next_ptr, &remaining);

  HT_ASSERT(matching <= m_bufs[current].fill());
  m_bufs[next].clear();
  m_bufs[next].ensure(8+matching+remaining);

  Serialization::encode_vi32(&m_bufs[next].ptr, 1+matching+remaining);
  *(m_bufs[next].ptr)++ = control;
  if (matching)
    memcpy(m_bufs[next].ptr, m_current_base, matching);
  m_current_base = m_bufs[next].ptr;
  m_bufs[next].ptr += matching;
  m_bufs[next].add_unchecked(next_ptr, remaining);
  m_serialized_key.ptr = m_bufs[next].base;
  m_first = !m_first;
  return next_ptr + remaining;
}


bool KeyDecompressorPrefix::less_than(SerializedKey serialized_key) {
  return m_serialized_key < serialized_key;
}


void KeyDecompressorPrefix::load(Key &key) {
  key.load(m_serialized_key);
}
