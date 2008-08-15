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

#include "Common/Compat.h"

#include <iostream>
#include <limits>
#include <fstream>
#include <string>

extern "C" {
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
}

#include "Logger.h"
#include "Properties.h"
#include "StringExt.h"

using namespace std;
using namespace Hypertable;


void Properties::load(const char *fname) throw(std::invalid_argument) {
  struct stat statbuf;
  char *name, *value, *last, *ptr, *linep;
  std::string valstr;

  if (stat(fname, &statbuf) != 0)
    throw std::invalid_argument(string("Could not stat properties file '")
        + fname + "' - " + string(strerror(errno)));

  ifstream ifs(fname);
  string line;
  while(getline(ifs, line)) {

    linep = (char *)line.c_str();
    while (*linep && isspace(*linep))
      linep++;

    if (*linep == 0 || *linep == '#')
      continue;

    if ((name = strtok_r(linep, "=", &last)) != 0) {
      if ((value = strtok_r(0, "=", &last)) != 0) {

        ptr = name + strlen(name);
        while (ptr > name && (isspace(*(ptr-1))))
          ptr--;

        if (ptr == name)
          continue;
        *ptr = 0;

        while (*value && isspace(*value))
          value++;

        if (*value == 0)
          continue;

        ptr = value + strlen(value);
        while (ptr > value && (isspace(*(ptr-1))))
          ptr--;

        if (ptr == value)
          continue;
        *ptr = 0;

        if (*value == '\'' && *(ptr-1) == '\'') {
          value++;
          *(ptr-1) = 0;
          if (*value == 0)
            continue;
        }
        else if (*value == '"' && *(ptr-1) == '"') {
          value++;
          *(ptr-1) = 0;
          if (*value == 0)
            continue;
        }

        valstr = value;

        m_map[name] = valstr;
      }
    }
  }
}

const char *Properties::get(const char *str, const char *defaultval) {
  PropertyMap::iterator iter =  m_map.find(str);
  if (iter == m_map.end())
    return defaultval;
  return ((*iter).second.c_str());
}


/**
 *
 */
int64_t Properties::get_int64(const char *str, int64_t defaultval) {

  PropertyMap::iterator iter = m_map.find(str);
  if (iter == m_map.end())
    return defaultval;

  return parse_int_value(str, (*iter).second);
}

int Properties::get_int(const char *str, int defaultval) {
  int64_t llval = get_int64(str, defaultval);

  if (llval > numeric_limits<int>::max())
    throw std::invalid_argument(string("Integer value too large for property '")
                                + str + "'");

  return (int)llval;
}

bool Properties::get_bool(const char *str, bool defaultval) {
  PropertyMap::iterator iter = m_map.find(str);
  if (iter == m_map.end())
    return defaultval;

  if (!strcasecmp((*iter).second.c_str(), "true")
      || !strcmp((*iter).second.c_str(), "1"))
    return true;
  else if (!strcasecmp((*iter).second.c_str(), "false")
           || !strcmp((*iter).second.c_str(), "0"))
    return false;

  HT_ERRORF("Invalid value for property '%s' (%s), using defaultval value",
            str, (*iter).second.c_str());

  return defaultval;
}



std::string Properties::set(const char *key, const char *value) {
  std::string oldval;

  PropertyMap::iterator iter = m_map.find(key);
  if (iter != m_map.end())
    oldval = (*iter).second;

  m_map[key] = value;

  return oldval;
}


int Properties::set_int(const String &key, int value) {
  int64_t old_val = 0;

  if (value > numeric_limits<int>::max())
    throw std::invalid_argument(string("Integer value too large for property '")
                                + key + "'");

  PropertyMap::iterator iter = m_map.find(key);
  if (iter != m_map.end()) {
    old_val = parse_int_value(key, (*iter).second);
    if (old_val > numeric_limits<int>::max())
      throw std::invalid_argument(string("Integer value too large for property "
                                  "'") + key + "'");
  }

  m_map[key] = std::string("") + value;

  return old_val;
}


int64_t Properties::set_int64(const String &key, int64_t value) {
  int64_t old_val = 0;

  PropertyMap::iterator iter = m_map.find(key);
  if (iter != m_map.end())
    old_val = parse_int_value(key, (*iter).second);

  m_map[key] = std::string("") + (long long)value;

  return old_val;
}


int64_t Properties::parse_int_value(const String &key, const String &value) {
  const char *ptr;

  for (ptr = value.c_str(); isdigit(*ptr); ptr++)
    ;

  uint64_t factor = 1LL;
  if (*ptr != 0) {
    if (!strcasecmp(ptr, "k"))
      factor = 1000LL;
    else if (!strcasecmp(ptr, "m"))
      factor = 1000000LL;
    else if (!strcasecmp(ptr, "g"))
      factor = 1000000000LL;
    else
      throw std::invalid_argument(string("Invalid value for integer property '")
                                  + key + "' (value=" + value + ")");
  }

  string numstr = string(value.c_str(), ptr-value.c_str());

  int64_t llval = strtoll(numstr.c_str(), 0, 0);
  if (llval == 0 && errno == EINVAL)
    throw std::invalid_argument(string("Could not convert property '") + key
                                + "' (value=" + value + ") to an integer");

  return llval * factor;
}
