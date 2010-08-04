/** -*- c++ -*-
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#ifndef HYPERTABLE_NAMESPACELISTING_H
#define HYPERTABLE_NAMESPACELISTING_H

#include "Common/String.h"

namespace Hypertable {
  class NamespaceListing {
  public:
    String name;
    String id;
    bool is_namespace;
  };

  struct LtNamespaceListingName {
    bool operator()(const NamespaceListing &x, const NamespaceListing &y) const {
      int cmp = x.name.compare(y.name);
      if (cmp==0)
        return (int)x.is_namespace < (int)y.is_namespace;
      else if (cmp < 0)
        return true;
      else
        return false;
    }
  };

  struct LtNamespaceListingId {
    bool operator()(const NamespaceListing &x, const NamespaceListing &y) const {
      int cmp = x.id.compare(y.id);
      if (cmp==0)
        return (int)x.is_namespace < (int)y.is_namespace;
      else if (cmp < 0)
        return true;
      else
        return false;
    }
  };
}

#endif // HYPERTABLE_NAMESPACELISTING_H
