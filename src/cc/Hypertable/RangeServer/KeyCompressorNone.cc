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

#include "KeyCompressorNone.h"

using namespace Hypertable;


void KeyCompressorNone::reset() {
  m_serialized_key.ptr = 0;
  m_serialized_key_len = 0;
}

void KeyCompressorNone::add(const Key &key) {
  HT_ASSERT(key.serial.ptr);
  m_serialized_key = key.serial;
  m_serialized_key_len = key.serial.length();
}

size_t KeyCompressorNone::length() {
  return m_serialized_key_len;
}

size_t KeyCompressorNone::length_uncompressed() {
  return m_serialized_key_len;
}

void KeyCompressorNone::write(uint8_t *buf) {
  memcpy(buf, m_serialized_key.ptr, m_serialized_key_len);
  m_serialized_key.ptr = buf;
}

void KeyCompressorNone::write_uncompressed(uint8_t *buf) {
  memcpy(buf, m_serialized_key.ptr, m_serialized_key_len);
  m_serialized_key.ptr = buf;
}
