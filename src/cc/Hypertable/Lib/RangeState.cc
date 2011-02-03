/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include "RangeState.h"

#include "Common/Logger.h"
#include "Common/Serialization.h"

using namespace Hypertable;
using namespace Serialization;

void RangeState::clear() {
  state = STEADY;
  soft_limit = 0;
  transfer_log = split_point = old_boundary_row = 0;
}


size_t RangeState::encoded_length() const {
  return 9 + 8 + encoded_length_vstr(transfer_log) +
      encoded_length_vstr(split_point) + encoded_length_vstr(old_boundary_row);
}


void RangeState::encode(uint8_t **bufp) const {
  *(*bufp)++ = state;
  encode_i64(bufp, timestamp);
  encode_i64(bufp, soft_limit);
  encode_vstr(bufp, transfer_log);
  encode_vstr(bufp, split_point);
  encode_vstr(bufp, old_boundary_row);
}


void RangeState::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding range state",
    state = decode_byte(bufp, remainp);
    timestamp = decode_i64(bufp, remainp);
    soft_limit = decode_i64(bufp, remainp);
    transfer_log = decode_vstr(bufp, remainp);
    split_point = decode_vstr(bufp, remainp);
    old_boundary_row = decode_vstr(bufp, remainp));

}

void RangeStateManaged::clear() {
  RangeState::clear();
  *this = *this;
}


void RangeStateManaged::decode(const uint8_t **bufp, size_t *remainp) {
  RangeState::decode(bufp, remainp);
  *this = *this;
}

std::ostream& Hypertable::operator<<(std::ostream &out, const RangeState &st) {
  out <<"{RangeState: state=";

  switch (st.state) {
  case RangeState::STEADY: out <<"STEADY";              break;
  case RangeState::SPLIT_LOG_INSTALLED: out <<"SLI";    break;
  case RangeState::SPLIT_SHRUNK: out <<"SHRUNK";        break;
  default:
    out <<"unknown ("<< st.state;
  }
  out << " timestamp=" << st.timestamp;
  out <<" soft_limit="<< st.soft_limit;
  if (st.transfer_log)
    out <<" transfer_log='"<< st.transfer_log << "'";
  if (st.split_point)
    out <<" split_point='"<< st.split_point << "'";
  if (st.old_boundary_row)
    out <<" old_boundary_row='"<< st.old_boundary_row << "'";
  out <<"}";
  return out;
}
