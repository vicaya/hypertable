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

#ifndef HYPERTABLE_TIMESTAMP_H
#define HYPERTABLE_TIMESTAMP_H

#include "Common/Serialization.h"

namespace Hypertable {

  class Timestamp {
  public:
    Timestamp(int64_t l, int64_t r) : logical(l), real(r) { return; }
    Timestamp() : logical(0), real(0) { return; }
    void clear() { logical = real = 0; }
    size_t encoded_length() const { return 16; }
    void encode(uint8_t **bufp) const {
      Serialization::encode_i64(bufp, logical);
      Serialization::encode_i64(bufp, real);
    }
    void decode(const uint8_t **bufp, size_t *remainp) {
      logical = Serialization::decode_i64(bufp, remainp);
      real    = Serialization::decode_i64(bufp, remainp);
    }
    bool operator<(const Timestamp& ts) const { return logical<ts.logical; }
    int64_t logical;
    int64_t real;
  };

}

#endif // HYPERTABLE_TIMESTAMP_H
