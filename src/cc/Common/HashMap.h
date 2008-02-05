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

#ifndef HYPERTABLE_HASHMAP_H
#define HYPERTABLE_HASHMAP_H

#include <boost/config.hpp>
#include BOOST_HASH_MAP_HEADER
#include BOOST_HASH_SET_HEADER

namespace Hypertable {
  // import hash_map/set into our namespace
  using BOOST_STD_EXTENSION_NAMESPACE::hash_map;
  using BOOST_STD_EXTENSION_NAMESPACE::hash_set;
} // namespace Hypertable

#endif // HYPERTABLE_HASHMAP_H
