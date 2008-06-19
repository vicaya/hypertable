/**
 * Copyright (C) 2007 Luke Lu (Zvents, Inc.)
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

#include "Compat.h"
#include "Time.h"
#include "Mutex.h"

using namespace Hypertable;

uint64_t
Hypertable::get_ts64() {
  static Mutex mutex;
  ScopedLock lock(mutex);
  HiResTime now;
  return ((uint64_t)now.sec * 1000000000LL) + (uint64_t)now.nsec;
}
