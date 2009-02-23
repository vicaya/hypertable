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

#ifndef HYPERTABLE_LOOKUP3_H
#define HYPERTABLE_LOOKUP3_H

#include "Common/String.h"

namespace Hypertable {

/**
 * The lookup3 hash from Bob Jenkins, a fast and strong 32-bit hash
 */
uint32_t hashlittle(const void *data, size_t len, uint32_t hash);

struct Lookup3 {
  uint32_t
  operator()(const String& s) const {
    return hashlittle(s.c_str(), s.length(), s.length());
  }

  uint32_t
  operator()(const void *start, size_t len) const {
    return hashlittle(start, len, len);
  }

  uint32_t
  operator()(const void *start, size_t len, uint32_t init_hash) const {
    return hashlittle(start, len, init_hash);
  }
};

} // namespace Hypertable

#endif // HYPERTABLE_LOOKUP3_H
