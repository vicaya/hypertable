/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

#include "Common/Compat.h"
#include "Common/System.h"

#include "Hypertable/RangeServer/TableIdCache.h"

using namespace Hypertable;

int main(int argc, char **argv) {
  TableIdCachePtr cache;

  System::initialize(System::locate_install_dir(argv[0]));

  cache = new TableIdCache(50);

  for (int i=0; i<100; i++)
    cache->insert(i);

  for (int i=0; i<50; i++) {
    if (cache->contains(i))
      return 1;
  }

  for (int i=50; i<100; i++) {
    if (!cache->contains(i))
      return 1;
  }

  return 0;
}
