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

#include "KeyCompressorPrefix.h"

using namespace Hypertable;


void KeyCompressorPrefix::reset() {
  m_buffer.clear();
  m_compressed_key.clear();
  m_uncompressed_key.clear();
}

void KeyCompressorPrefix::add(const Key &key) {
  uint32_t payload_length = key.length - (((const uint8_t *)key.row)-key.serial.ptr) + 1;
  HT_ASSERT(key.serial.ptr);
  m_last_control = key.control;
  if (m_buffer.fill() == 0) {
    m_compressed_key.clear();
    m_compressed_key.ensure(payload_length+8);
    Serialization::encode_vi32(&m_compressed_key.ptr, payload_length+1);
    *(m_compressed_key.ptr)++ = key.control;
    *(m_compressed_key.ptr)++ = 0;
    m_suffix = m_compressed_key.ptr;
    m_suffix_length = payload_length-1;
    m_compressed_key.add_unchecked(key.row, m_suffix_length);
  }
  else {
    size_t n = std::min((size_t)(payload_length-1), m_buffer.fill());

    uint8_t *ptr = m_buffer.base;
    const uint8_t *end = m_buffer.base + n;
    const uint8_t *incoming = (const uint8_t *)key.row;
    const uint8_t *incoming_end = incoming + (payload_length-1);

    // match what's in the buffer
    while (ptr < end) {
      if (*ptr != *incoming)
        break;
      ptr++;
      incoming++;
    }

    // If the entire buffer matches,
    // match the suffix in the compressed key buffer
    if (ptr == m_buffer.ptr) {
      n = std::min(m_suffix_length, (size_t)(incoming_end-incoming));
      const uint8_t *base = m_suffix;
      end = base + n;
      while (m_suffix < end) {
        if (*m_suffix != *incoming)
          break;
        m_suffix++;
        incoming++;
      }
      if (m_suffix > base) {
        m_buffer.reserve(m_suffix-base);
        m_buffer.add(base, m_suffix-base);
      }
    }
    else
      m_buffer.ptr = ptr;

    m_suffix_length = incoming_end - incoming;

    uint32_t matching = m_buffer.fill();
    size_t encoding_bytes = Serialization::encoded_length_vi32(matching);
    uint32_t total_bytes = 1 + encoding_bytes + m_suffix_length;
      
    m_compressed_key.clear();
    Serialization::encode_vi32(&m_compressed_key.ptr, total_bytes);
    *m_compressed_key.ptr++ = key.control;
    Serialization::encode_vi32(&m_compressed_key.ptr, matching);
    memcpy(m_compressed_key.ptr, m_suffix, m_suffix_length);
    m_suffix = m_compressed_key.ptr;
    m_compressed_key.ptr += m_suffix_length;
  }

}

size_t KeyCompressorPrefix::length() {
  return m_compressed_key.fill();
}

size_t KeyCompressorPrefix::length_uncompressed() {
  if (m_uncompressed_key.empty())
    render_uncompressed();
  return m_uncompressed_key.fill();
}

void KeyCompressorPrefix::write(uint8_t *buf) {
  memcpy(buf, m_compressed_key.base, m_compressed_key.fill());
}

void KeyCompressorPrefix::write_uncompressed(uint8_t *buf) {
  if (m_uncompressed_key.empty())
    render_uncompressed();
  memcpy(buf, m_uncompressed_key.base, m_uncompressed_key.fill());
}

void KeyCompressorPrefix::render_uncompressed() {
  uint32_t length = 1 + m_buffer.fill() + m_suffix_length;
  size_t encoding_bytes = Serialization::encoded_length_vi32(length);

  m_uncompressed_key.clear();
  m_uncompressed_key.ensure(encoding_bytes + length);
  Serialization::encode_vi32(&m_uncompressed_key.ptr, length);
  *(m_uncompressed_key.ptr)++ = m_last_control;
  m_uncompressed_key.add_unchecked(m_buffer.base, m_buffer.fill());
  m_uncompressed_key.add_unchecked(m_suffix, m_suffix_length);
}
