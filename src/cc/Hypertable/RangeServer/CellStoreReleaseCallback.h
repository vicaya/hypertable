/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_CELLSTORERELEASECALLBACK_H
#define HYPERTABLE_CELLSTORERELEASECALLBACK_H

#include <iostream>

#include <vector>

#include "Common/String.h"
#include "Common/Logger.h"

namespace Hypertable {

  class AccessGroup;

  class CellStoreReleaseCallback {
  public:
    CellStoreReleaseCallback() : m_access_group(0) { return; }
    CellStoreReleaseCallback(AccessGroup *ag);

    void operator()() const;

    operator bool () const {
      return m_access_group != 0;
    }

    void add_file(String &filename) {
      HT_DEBUG_OUT<< "WOW pushing back " << filename << HT_END;
      m_filenames.push_back(filename);
    }

  private:
    AccessGroup *m_access_group;
    std::vector<String> m_filenames;
  };

}

#endif // HYPERTABLE_CELLSTORERELEASECALLBACK_H
