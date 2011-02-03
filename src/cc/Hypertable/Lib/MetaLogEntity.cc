/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
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
#include "Common/Compat.h"
#include "Common/Checksum.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Serialization.h"

#include "MetaLogEntity.h"

using namespace Hypertable;
using namespace Hypertable::MetaLog;

Entity::Entity(int32_t type) : header(type) { }

Entity::Entity(const EntityHeader &header_) : header(header_) { }


void Entity::encode_entry(uint8_t **bufp) {
  header.length = encoded_length();
  uint8_t *header_encode_position = *bufp;
  *bufp += EntityHeader::LENGTH;
  uint8_t *payload_encode_position = *bufp;
  encode(bufp);
  header.checksum = fletcher32(payload_encode_position, *bufp-payload_encode_position);
  header.encode(&header_encode_position);
  HT_ASSERT(header_encode_position == payload_encode_position);
}

void Entity::decode_entry(const uint8_t **bufp, size_t *remainp) {
  header.decode(bufp, remainp);
  const uint8_t *base = *bufp;
  decode(bufp, remainp);
  int32_t checksum = (int32_t)fletcher32(base, *bufp-base);
  if (checksum != header.checksum)
    HT_THROWF(Error::METALOG_CHECKSUM_MISMATCH,
              "MetaLog checksum mismatch header=%lx, computed=%lx",
              (Lu)header.checksum, (Lu)checksum);
}
