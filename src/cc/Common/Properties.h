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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_PROPERTIES_H
#define HYPERTABLE_PROPERTIES_H

#include <ext/hash_map>

#include <boost/intrusive_ptr.hpp>

#include "ReferenceCount.h"
#include "StringExt.h"

namespace Hypertable {

  class Properties : public ReferenceCount {

  public:

    Properties() { return; }

    Properties(std::string fname) {
      load(fname);
    }

    void load(const char *fname) throw(std::invalid_argument);

    void load(std::string &fname) { load(fname.c_str());  }

    const char *get(const char *str, const char *defaultValue = NULL);

    int get_int(const char *str, int defaultValue = 0);

    int64_t get_int64(const char *str, int64_t defaultValue = 0);

    bool get_bool(const char *str, bool defaultValue = false);

    bool has_key(const char *str) {
      PropertyMapT::iterator iter = m_map.find(str);
      return (iter == m_map.end()) ? false : true;
    }

    std::string set(const char *key, const char *value);

  private:

    typedef __gnu_cxx::hash_map<std::string, std::string> PropertyMapT;

    PropertyMapT  m_map;
  };
  typedef boost::intrusive_ptr<Properties> PropertiesPtr;

}

#endif // HYPERTABLE_PROPERTIES_H
