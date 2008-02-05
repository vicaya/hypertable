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

using namespace std;
using namespace Hypertable;


void Properties::load(const char *fname) throw(std::invalid_argument) {
  struct stat statbuf;
  char *name, *value, *last, *ptr, *lineChars;
  std::string valueStr;

  if (stat(fname, &statbuf) != 0)
    throw std::invalid_argument(string("Could not stat properties file '") + fname + "' - " + string(strerror(errno)));

  ifstream ifs(fname);
  string line;
  while(getline(ifs, line)) {

    lineChars = (char *)line.c_str();
    while (*lineChars && isspace(*lineChars))
      lineChars++;

    if (*lineChars == 0 || *lineChars == '#')
      continue;

    if ((name = strtok_r(lineChars, "=", &last)) != 0) { 
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

	valueStr = value;

	m_map[name] = valueStr;
      }
    }
  }
}

const char *Properties::get(const char *str, const char *defaultValue) {
  PropertyMapT::iterator iter =  m_map.find(str);
  if (iter == m_map.end())
    return defaultValue;
  return ((*iter).second.c_str());
}


/**
 *
 */
int64_t Properties::get_int64(const char *str, int64_t defaultValue) {
  const char *ptr;

  PropertyMapT::iterator iter = m_map.find(str);
  if (iter == m_map.end())
    return defaultValue;

  for (ptr = (*iter).second.c_str(); isdigit(*ptr); ptr++)
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
      throw std::invalid_argument(string("Invalid value for integer property '") + str + "' (value=" + (*iter).second + ")");
  }

  string numericStr = string((*iter).second.c_str(), ptr-(*iter).second.c_str());

  int64_t llval = strtoll(numericStr.c_str(), 0, 0);
  if (llval == 0 && errno == EINVAL)
    throw std::invalid_argument(string("Could not convert property '") + str + "' (value=" + (*iter).second + ") to an integer");

  return llval * factor;
}

int Properties::get_int(const char *str, int defaultValue) {
  int64_t llval = get_int64(str, defaultValue);
  
  if (llval > numeric_limits<int>::max())
    throw std::invalid_argument(string("Integer value too large for property '") + str + "'");

  return (int)llval;
}

bool Properties::get_bool(const char *str, bool defaultValue) {
  PropertyMapT::iterator iter = m_map.find(str);
  if (iter == m_map.end())
    return defaultValue;

  if (!strcasecmp((*iter).second.c_str(), "true") || !strcmp((*iter).second.c_str(), "1"))
    return true;
  else if (!strcasecmp((*iter).second.c_str(), "false") || !strcmp((*iter).second.c_str(), "0"))
    return false;

  HT_ERRORF("Invalid value for property '%s' (%s), using default value", str, (*iter).second.c_str());

  return defaultValue;
}



std::string Properties::set(const char *key, const char *value) {
  std::string oldValue;

  PropertyMapT::iterator iter = m_map.find(key);
  if (iter != m_map.end())
    oldValue = (*iter).second;

  m_map[key] = value;

  return oldValue;
}
