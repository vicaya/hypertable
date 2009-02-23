/**
 * Copyright (C) 2007 Naveen Koorakula
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
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

#ifndef HYPERTABLE_MURMURHASH_H
#define HYPERTABLE_MURMURHASH_H

#include "Common/String.h"

namespace Hypertable {

/**
 * The MurmurHash 2 from Austin Appleby, faster and better mixed (but weaker
 * crypto-wise with one pair of obvious differential) than both Lookup3 and
 * SuperFastHash. Not-endian neutral for speed.
 */
uint32_t murmurhash2(const void *data, size_t len, uint32_t hash);

struct MurmurHash2 {
  uint32_t operator()(const String& s) const {
    return murmurhash2(s.c_str(), s.length(), 0);
  }

  uint32_t operator()(const void *start, size_t len) const {
    return murmurhash2(start, len, 0);
  }

  uint32_t operator()(const void *start, size_t len, uint32_t seed) const {
    return murmurhash2(start, len, seed);
  }

  uint32_t operator()(const char *s) const {
    return murmurhash2(s, strlen(s), 0);
  }
};

} // namespace Hypertable

#endif // HYPERTABLE_MURMURHASH_H
