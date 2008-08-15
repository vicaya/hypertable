/** -*- c++ -*-
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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_FLYWEIGHTSTRING_H
#define HYPERTABLE_FLYWEIGHTSTRING_H

#include <set>

#include "StringExt.h"

namespace Hypertable {

  class FlyweightString {
  public:
    ~FlyweightString() {
      for (Strings::iterator iter = m_strings.begin(); iter != m_strings.end();
           ++iter)
        delete [] *iter;
    }
    const char *get(const char *str) {
      Strings::iterator iter = m_strings.find(str);
      if (iter != m_strings.end())
        return *iter;
      char *constant_str = new char [strlen(str) + 1];
      strcpy(constant_str, str);
      m_strings.insert(constant_str);
      return constant_str;
    }

  private:
    typedef std::set<const char *, LtCstr>  Strings;

    Strings m_strings;
  };

}

#endif // HYPERTABLE_FLYWEIGHTSTRING_H
