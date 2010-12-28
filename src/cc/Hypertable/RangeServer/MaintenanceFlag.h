/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Hypertable, Inc.)
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

#ifndef HYPERTABLE_MAINTENANCEFLAG_H
#define HYPERTABLE_MAINTENANCEFLAG_H

#include "Common/HashMap.h"

namespace Hypertable {
  namespace MaintenanceFlag {

    enum {
      SPLIT                     = 0x0100,
      COMPACT                   = 0x0200,
      COMPACT_MINOR             = 0x0201,
      COMPACT_MAJOR             = 0x0202,
      COMPACT_MERGING           = 0x0204,
      COMPACT_GC                = 0x0205,
      MEMORY_PURGE              = 0x0400,
      MEMORY_PURGE_SHADOW_CACHE = 0x0401,
      MEMORY_PURGE_CELLSTORE    = 0x0402,
      RELINQUISH                = 0x0800
    };

    inline bool minor_compaction(int flags) {
      return (flags & COMPACT_MINOR) == COMPACT_MINOR;
    }

    inline bool major_compaction(int flags) {
      return (flags & COMPACT_MAJOR) == COMPACT_MAJOR;
    }

    inline bool gc_compaction(int flags) {
      return (flags & COMPACT_GC) == COMPACT_GC;
    }

    inline bool purge_shadow_cache(int flags) {
      return (flags & MEMORY_PURGE_SHADOW_CACHE) == MEMORY_PURGE_SHADOW_CACHE;
    }

    inline bool purge_cellstore(int flags) {
      return (flags & MEMORY_PURGE_CELLSTORE) == MEMORY_PURGE_CELLSTORE;
    }

    class Hash {
    public:
      size_t operator () (const void *obj) const {
	return (size_t)obj;
      }
    };

    struct Equal {
      bool operator()(const void *obj1, const void *obj2) const {
	return obj1 == obj2;
      }
    };

    
    class Map : public hash_map<const void *, int, Hash, Equal> {
    public:
      int flags(const void *key) {
	iterator iter = this->find(key);
	if (iter != this->end())
	  return (*iter).second;
	return 0;
      }
      bool minor_compaction(const void *key) {
	iterator iter = this->find(key);
	if (iter != this->end())
	  return ((*iter).second & COMPACT_MINOR) == COMPACT_MINOR;
	return false;
      }
      bool memory_purge(const void *key) {
	iterator iter = this->find(key);
	if (iter != this->end())
	  return ((*iter).second & MEMORY_PURGE) == MEMORY_PURGE;
	return false;
      }
    };
  }
}

#endif // HYPERTABLE_MAINTENANCEFLAG_H
