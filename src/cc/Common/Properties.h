/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_PROPERTIES_H
#define HYPERTABLE_PROPERTIES_H

#include <boost/intrusive_ptr.hpp>

#include "ReferenceCount.h"
#include "StringExt.h"

namespace Hypertable {

  class Properties : public ReferenceCount {

  public:

    Properties() { return; }

    Properties(const String &fname) {
      load(fname);
    }

    void load(const char *fname) throw(std::invalid_argument);

    void load(const String &fname) { load(fname.c_str());  }

    const char *get(const char *str, const char *defaultval = NULL);

    int get_int(const char *str, int defaultval = 0);

    int64_t get_int64(const char *str, int64_t defaultval = 0);

    bool get_bool(const char *str, bool defaultval = false);

    bool has_key(const char *str) {
      PropertyMap::iterator iter = m_map.find(str);
      return (iter == m_map.end()) ? false : true;
    }

    String set(const char *key, const char *value);

    int set_int(const String &key, int value);

    int64_t set_int64(const String &key, int64_t value);

  private:

    int64_t parse_int_value(const String &key, const String &value);
    typedef hash_map<String, String> PropertyMap;

    PropertyMap  m_map;
  };
  typedef boost::intrusive_ptr<Properties> PropertiesPtr;

}

#endif // HYPERTABLE_PROPERTIES_H
