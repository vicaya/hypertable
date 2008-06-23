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

#include "Common/Compat.h"
#include "RangeState.h"

#include "Common/Serialization.h"

using namespace Hypertable;
using namespace Serialization;

size_t RangeState::encoded_length() {
  return 9 + encoded_length_vstr(transfer_log);
}


void RangeState::encode(uint8_t **bufp) {
  *(*bufp)++ = state;
  encode_i64(bufp, soft_limit);
  encode_vstr(bufp, transfer_log);
}


void RangeState::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding range state",
    state = decode_byte(bufp, remainp);
    soft_limit = decode_i64(bufp, remainp);
    transfer_log = decode_vstr(bufp, remainp));
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
  out <<" soft_limit="<< st.soft_limit <<" transfer_log='"<< st.transfer_log
      <<"'}";
  return out;
}
