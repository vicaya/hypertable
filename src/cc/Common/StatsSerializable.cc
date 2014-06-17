/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
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

#include "Compat.h"
#include "Logger.h"
#include "Serialization.h"
#include "StatsSerializable.h"

using namespace Hypertable;

StatsSerializable::StatsSerializable(uint16_t _id, uint8_t _group_count) : id(_id), group_count(_group_count) {
  HT_ASSERT(group_count < 32);
}

StatsSerializable::StatsSerializable(const StatsSerializable &other) {
  id = other.id;
  group_count = other.group_count;
  memcpy(group_ids, other.group_ids, 32);
}

StatsSerializable &StatsSerializable::operator=(const StatsSerializable &other) {
  id = other.id;
  group_count = other.group_count;
  memcpy(group_ids, other.group_ids, 32);
  return *this;
}

size_t StatsSerializable::encoded_length() const {
  size_t len = 2;
  for (uint8_t i=0; i<group_count; i++) {
    len += 3;
    len += encoded_length_group(group_ids[i]);
  }
  return len;
}

void StatsSerializable::encode(uint8_t **bufp) const {
  uint8_t *lenp;
  uint16_t len;

  *(*bufp)++ = id;
  *(*bufp)++ = group_count;

  for (uint8_t i=0; i<group_count; i++) {
    *(*bufp)++ = group_ids[i];
    lenp = *bufp;
    (*bufp) += 2;
    encode_group(group_ids[i], bufp);
    len = ((*bufp) - lenp) - 2;
    Serialization::encode_i16(&lenp, len);
  }
  return;
}

void StatsSerializable::decode(const uint8_t **bufp, size_t *remainp) {
  uint16_t len;

  // Read Section indicator
  if (*remainp == 0)
    HT_THROW_INPUT_OVERRUN(*remainp, 1);
  HT_ASSERT(*(*bufp) == id);
  (*bufp)++;
  (*remainp)--;

  // Read Group Count
  if (*remainp == 0)
    HT_THROW_INPUT_OVERRUN(*remainp, 1);
  group_count = (size_t)*(*bufp)++;
  (*remainp)--;
  
  for (uint8_t i=0; i<group_count; i++) {

    // Read Group ID
    if (*remainp == 0)
      HT_THROW_INPUT_OVERRUN(*remainp, 1);
    group_ids[i] = *(*bufp)++;
    (*remainp)--;    

    // Read Group length
    if (*remainp < 2)
      HT_THROW_INPUT_OVERRUN(*remainp, 2);
    len = Serialization::decode_i16(bufp, remainp);
    if (*remainp < len)
      HT_THROW_INPUT_OVERRUN(*remainp, len);

    decode_group(group_ids[i], len, bufp, remainp);

  }

}

bool StatsSerializable::operator==(const StatsSerializable &other) const {
  if (id == other.id &&
      group_count == other.group_count &&
      !memcmp(group_ids, other.group_ids, group_count))
    return true;
  return false;
}
