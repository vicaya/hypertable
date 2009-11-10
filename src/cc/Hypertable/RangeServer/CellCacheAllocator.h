/** -*- C++ -*-
 * Copyright (C) 2009  Luke Lu (llu@hypertable.org)
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

#ifndef HYPERTABLE_CELLCACHE_ALLOCATOR_H
#define HYPERTABLE_CELLCACHE_ALLOCATOR_H

#include "Common/PageArenaAllocator.h"

namespace Hypertable {

struct CellCachePageAllocator : DefaultPageAllocator {
  void *allocate(size_t sz);
  void freed(size_t sz);
};

typedef PageArena<uint8_t, CellCachePageAllocator> CellCacheArena;

template <typename T, class Impl = PageArenaAllocator<T, CellCacheArena> >
struct CellCacheAllocator : Impl {
  template <typename U>
  struct rebind { typedef PageArenaAllocator<U, CellCacheArena> other; };

  CellCacheAllocator() {}
  CellCacheAllocator(CellCacheArena &arena) : Impl(arena) {}
};

} // namespace Hypertable

#endif /* HYPERTABLE_CELLCACHE_ALLOCATOR_H */
