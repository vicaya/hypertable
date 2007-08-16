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
#include <string>

#include <cstring>

#include "InteractiveCommand.h"
#include "Usage.h"

using namespace hypertable;
using namespace std;

/**
 *
 */
void InteractiveCommand::ParseCommandLine(const char *line) {
  size_t commandLen = strlen(this->CommandText());
  const char *base, *ptr;
  int lastchar = 0;
  string key, value;

  assert(!strncmp(line, CommandText(), commandLen));

  mArgs.clear();

  ptr = line + commandLen;

  while (true) {

    key = "";
    value = "";

    // skip whitespace
    while (*ptr && isspace(*ptr))
      ptr++;
    if (*ptr == 0)
      break;

    if (*ptr == '\"') {
      if (!ParseStringLiteral(ptr, key, &ptr))
	exit(1);
    }
    else {
      base = ptr;
      while (true) {
	if (*ptr == '=') {
	  key = string(base, ptr-base);
	  base = ++ptr;
	  if (*base == '\"') {
	    if (!ParseStringLiteral(base, value, &ptr))
	      exit(1);
	    break;
	  }
	  else {
	    for (ptr=base; *ptr && !isspace(*ptr); ptr++)
	      ;
	    value = string(base, ptr-base);
	    break;
	  }
	}
	else if (*ptr == 0 || isspace(*ptr)) {
	  key = string(base, ptr-base);
	  break;
	}
	ptr++;
      }
    }
    mArgs.push_back( pair<string, string>(key, value) );
  }

}



bool InteractiveCommand::ParseStringLiteral(const char *str, std::string &text, const char **endptr) {
  int lastchar = *str;
  const char *ptr, *base;

  assert(*str == '"');

  base = str+1;

  for (ptr = base; *ptr; ++ptr) {
    if (*ptr == '\"' && lastchar != '\\')
      break;
    lastchar = *ptr;
  }

  if (*ptr == 0) {
    cerr << "Un-terminated string literal." << endl;
    return false;
  }

  text = string(base, ptr-base);

  *endptr = ptr+1;
  return true;
}
