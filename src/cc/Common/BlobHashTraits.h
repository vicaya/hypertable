/**
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef HYPERTABLE_BLOB_HASH_TRAITS_H
#define HYPERTABLE_BLOB_HASH_TRAITS_H

#include "CharArena.h"
#include "Common/MurmurHash.h"

namespace Hypertable {

struct Blob {
  Blob(const void *buf, size_t len) : start(buf), size(len) { }

  const void *start;
  size_t size;
};

/**
 * Traits for BlobHashMap/Set
 * Hash function default to MurmurHash2 for speed and mix
 */
template <typename HashFunT = MurmurHash2>
struct BlobHashTraits {
  typedef CharArena key_allocator;

  struct hasher {
    HashFunT hash_fun;

    size_t operator()(const Blob &b) const {
      return hash_fun(b.start, b.size);
    }
  };

  struct key_equal {
    bool operator()(const Blob &x, const Blob &y) const {
      if (x.size != y.size)
        return false;

      return memcmp(x.start, y.start, x.size) == 0;
    }
  };
};

} // namespace Hypertable

#endif // !HYPERTABLE_BLOB_HASH_TRAITS_H
