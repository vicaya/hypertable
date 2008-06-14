/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef DBTMANAGED_H
#define DBTMANAGED_H

#include <cstdlib>

#include <db_cxx.h>

namespace Hyperspace {

  class DbtManaged : public Dbt {
  public:
    DbtManaged() {
      clear();
      set_flags(DB_DBT_REALLOC);
    }
    ~DbtManaged() {
      if (get_data() != 0)
	free(get_data());
    }
    void set_str(const std::string &str) {
      size_t size = str.length() + 1;
      if (get_data() != 0)
	free(get_data());
      char *data = (char *)malloc(size);
      strcpy(data, str.c_str());
      set_data((void *)data);
      set_size(size);
    }
    const char *get_str() {
      return (const char *)get_data();
    }
    void clear() {
      if (get_data() != 0)
	free(get_data());
      set_data(0);
      set_size(0);
    }

  };

}

#endif // DBTMANAGED_H
