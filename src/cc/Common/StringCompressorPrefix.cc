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

#include "StringCompressorPrefix.h"
#include "Serialization.h"

using namespace Hypertable;

void StringCompressorPrefix::reset() {
  m_last_string.clear();
  m_compressed_string.clear();
}

void StringCompressorPrefix::add(const char *str) {
  m_uncompressed_len = strlen(str);
  size_t prefix_len = get_prefix_length(str, m_uncompressed_len);
  size_t suffix_len;
  // null terminate suffix
  if (prefix_len > m_uncompressed_len)
    suffix_len = 1;
  else
    suffix_len = m_uncompressed_len  - prefix_len + 1;

  m_compressed_len = Serialization::encoded_length_vi32(prefix_len) + suffix_len;

  m_compressed_string.clear();
  m_compressed_string.reserve(m_compressed_len);
  Serialization::encode_vi32(&m_compressed_string.ptr, prefix_len);
  memcpy(m_compressed_string.ptr, (const void*) (str+prefix_len), suffix_len);
  m_last_string = str;
}

size_t StringCompressorPrefix::get_prefix_length(const char *str, size_t len) {
  len = len > m_last_string.length() ? m_last_string.length() : len;
  size_t ii;

  for (ii=0; ii<len; ++ii)
    if (m_last_string[ii] != str[ii])
      break;
  return ii;
}

size_t StringCompressorPrefix::length() {
  return m_compressed_len;
}

size_t StringCompressorPrefix::length_uncompressed() {
  return m_uncompressed_len;
}

void StringCompressorPrefix::write(uint8_t *buf) {
  memcpy(buf, (const void *)m_compressed_string.base, m_compressed_len);
}

void StringCompressorPrefix::write_uncompressed(uint8_t *buf) {
  // null terminated string
  memcpy(buf, (const void *)m_last_string.c_str(), m_last_string.length()+1);
}
