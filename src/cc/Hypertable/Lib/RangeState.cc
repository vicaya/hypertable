/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#include "RangeState.h"

#include "Common/Serialization.h"

using namespace Hypertable;

size_t RangeState::encoded_length() {
  return 9 + Serialization::encoded_length_string(transfer_log);
}



void RangeState::encode(uint8_t **bufPtr) {
  *(*bufPtr)++ = state;
  Serialization::encode_long(bufPtr, soft_limit);
  Serialization::encode_string(bufPtr, transfer_log);
}



bool RangeState::decode(uint8_t **bufPtr, size_t *remainingPtr) {
  uint8_t bval;
  if (!Serialization::decode_byte(bufPtr, remainingPtr, &bval))
    return false;
  state = bval;
  if (!Serialization::decode_long(bufPtr, remainingPtr, &soft_limit))
    return false;
  if (!Serialization::decode_string(bufPtr, remainingPtr, &transfer_log))
    return false;
  return true;
}
