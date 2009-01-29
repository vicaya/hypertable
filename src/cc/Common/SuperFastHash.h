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

#ifndef INCLUDE_SUPERFASTHASH_H
#define INCLUDE_SUPERFASTHASH_H

#include <stdint.h>
#include <string>

#include "Common/StaticBuffer.h"

uint32_t SuperFastHash (const char *data, int len, uint32_t hash);

struct StringSuperFastHashTraits {
  struct hasher {
    size_t
    operator()(const std::string& s) const {
      return SuperFastHash(s.c_str(), s.length(), s.length());
    }

    size_t
    operator()(const Hypertable::StaticBuffer& s) const {
      return SuperFastHash((const char *)s.base, s.size, s.size);
    }
  };
};

#endif
