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

#include "KeyDecompressorNone.h"

using namespace Hypertable;


void KeyDecompressorNone::reset() {
  m_serialized_key.ptr = 0;
}

const uint8_t *KeyDecompressorNone::add(const uint8_t *ptr) {
  m_serialized_key.ptr = ptr;
  return ptr + m_serialized_key.length();
}


bool KeyDecompressorNone::less_than(SerializedKey serialized_key) {
  return m_serialized_key < serialized_key;
}


void KeyDecompressorNone::load(Key &key) {
  key.load(m_serialized_key);
}
