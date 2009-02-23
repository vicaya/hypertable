/**
 * Copyright (C) 2009 Luke Lu (Zvents Inc.)
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

#ifndef HYPERTABLE_TCLHASH_H
#define HYPERTABLE_TCLHASH_H

#include "Common/String.h"

namespace Hypertable {

/**
 * The Tcl hash by John Ousterhout
 * Fast and acceptable for alpha numeric keys
 */
inline size_t tcl_hash(const char *s) {
  register size_t ret = 0;

  for (; *s; ++s)
    ret += (ret << 3) + (unsigned)*s;

  return ret;
}

/**
 * Tcl hash for data with known length
 */
inline size_t tcl_hash(const void *data, size_t len, size_t seed) {
  register size_t ret = seed;
  register const uint8_t *dp = (uint8_t *)data, *end = dp + len;

  for (; dp < end; ++dp)
    ret += (ret << 3) + *dp;

  return ret;
}

#define HT_TCLHASH_DO2(p, i) \
  ret += (ret << 3) + p[i]; ret += (ret << 3) + p[i+1]

#define HT_TCLHASH_DO4(p, i) HT_TCLHASH_DO2(p, i); HT_TCLHASH_DO2(p, i+2);
#define HT_TCLHASH_DO8(p, i) HT_TCLHASH_DO4(p, i); HT_TCLHASH_DO4(p, i+4);

/**
 * Unrolled Tcl hash, up to 20% faster for longer (> 8 bytes) strings
 */
inline size_t tcl_hash2(const void *data, size_t len, size_t seed) {
  register size_t ret = seed;
  register const uint8_t *dp = (uint8_t *)data;

  if (len >= 8) do {
    HT_TCLHASH_DO8(dp, 0);
    dp += 8;
    len -= 8;
  } while (len >= 8);

  if (len > 0) do {
    ret += (ret << 3) + *dp++;
  } while (--len);

  return ret;
}

struct TclHash {
  size_t operator()(const void *start, size_t len, size_t seed) const {
    return tcl_hash(start, len, seed);
  }

  size_t operator()(const void *start, size_t len) const {
    return tcl_hash(start, len, 0);
  }

  size_t operator()(const String& s) const {
    return tcl_hash(s.c_str(), s.length(), 0);
  }

  size_t operator()(const char *s) const { return tcl_hash(s); }
};

struct TclHash2 {
  size_t operator()(const void *start, size_t len, size_t seed) const {
    return tcl_hash2(start, len, seed);
  }

  size_t operator()(const void *start, size_t len) const {
    return tcl_hash2(start, len, 0);
  }

  size_t operator()(const String& s) const {
    return tcl_hash2(s.c_str(), s.length(), 0);
  }

  size_t operator()(const char *s) const {
    return tcl_hash2(s, strlen(s), 0);
  }
};

} // namespace Hypertable

#endif // HYPERTABLE_TCLHASH_H
