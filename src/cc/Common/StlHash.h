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

#ifndef HYPERTABLE_STLHASH_H
#define HYPERTABLE_STLHASH_H

#include "Common/String.h"

namespace Hypertable {

/**
 * Hash function for strings found in SGI STL code
 * Slower than TclHash, included for comparison purpose
 */
inline size_t sgi_hash(const char *s) {
  register size_t ret = 0;

  for (; *s; ++s)
    ret = ret * 5 + (unsigned)*s;

  return ret;
}

inline size_t sgi_hash(const void *data, size_t len, size_t seed) {
  register size_t ret = seed;
  register const uint8_t *dp = (uint8_t *)data, *end = dp + len;

  for (; dp < end; ++dp)
    ret = ret * 5 + *dp;

  return ret;
}

/**
 * Hash function for strings found in STL TR1 code
 * Slower than TclHash and SgiHash, included for comparison purpose
 */
inline size_t tr1_hash(const char *s) {
  register size_t ret = 0;

  for (; *s; ++s)
    ret = ret * 131 + (unsigned)*s;

  return ret;
}

inline size_t tr1_hash(const void *data, size_t len, size_t seed) {
  register size_t ret = seed;
  register const uint8_t *dp = (uint8_t *)data, *end = dp + len;

  for (; dp < end; ++dp)
    ret = ret * 131 + *dp;

  return ret;
}

struct SgiHash {
  size_t operator()(const void *start, size_t len, size_t seed) const {
    return sgi_hash(start, len, seed);
  }

  size_t operator()(const void *start, size_t len) const {
    return sgi_hash(start, len, 0);
  }

  size_t operator()(const String& s) const {
    return sgi_hash(s.c_str(), s.length(), 0);
  }

  size_t operator()(const char *s) const { return sgi_hash(s); }
};

struct Tr1Hash {
  size_t operator()(const void *start, size_t len, size_t seed) const {
    return tr1_hash(start, len, seed);
  }

  size_t operator()(const void *start, size_t len) const {
    return tr1_hash(start, len, 0);
  }

  size_t operator()(const String& s) const {
    return tr1_hash(s.c_str(), s.length(), 0);
  }

  size_t operator()(const char *s) const { return tr1_hash(s); }
};

} // namespace Hypertable

#endif // HYPERTABLE_STLHASH_H
