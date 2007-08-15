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

  assert(!strncmp(line, CommandText(), commandLen));

  mArgs.clear();

  ptr = line + commandLen;

  while (true) {

    // skip whitespace
    while (*ptr && isspace(*ptr))
      ptr++;
    if (*ptr == 0)
      break;

    if (*ptr == '\"') {
      base = ptr+1;
      ptr = strchr(base, '\"');
      if (ptr == 0)
	Usage::DumpAndExit(Usage());
      mArgs.push_back( pair<string, string>("", string(base, ptr-base)) );
    }

    break;
  }
}
