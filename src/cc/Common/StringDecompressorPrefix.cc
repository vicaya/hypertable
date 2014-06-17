/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#include "Compat.h"

#include "StringDecompressorPrefix.h"
#include "Serialization.h"

using namespace Hypertable;

void StringDecompressorPrefix::reset() {
  m_last_string.clear();
  m_compressed_len = 0;
}

const uint8_t *StringDecompressorPrefix::add(const uint8_t *next_base) {
  String str;
  const uint8_t *next_ptr=next_base;
  size_t prefix_len = Serialization::decode_vi32(&next_ptr);
  str = m_last_string.substr(0, prefix_len);
  size_t suffix_len = strlen((const char*)next_ptr);
  str.append((const char *)next_ptr);
  m_last_string = str;
  const uint8_t *next = next_ptr + suffix_len +1;
  m_compressed_len = (size_t)(next - next_base);
  return (next_ptr + suffix_len +1);
}

size_t StringDecompressorPrefix::length() {
  return m_compressed_len;
}

size_t StringDecompressorPrefix::length_uncompressed() {
  return m_last_string.size();
}

void StringDecompressorPrefix::load(String &str) {
  str = m_last_string;
}
