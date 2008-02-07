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

#include <boost/algorithm/string.hpp>

#include "Hypertable/Lib/Key.h"

#include "ParseRangeSpec.h"

namespace Hypertable {

  bool ParseRangeSpec(std::string &spec, std::string &tableName, std::string &startRow, std::string &endRow) {
    const char *base = spec.c_str();
    const char *ptr, *leftBracket, *dotdot, *rightBracket;

    if ((leftBracket = strchr(base, '[')) == 0)
      return false;

    if ((dotdot = strstr(leftBracket, "..")) == 0)
      return false;

    if ((rightBracket = strchr(dotdot, ']')) == 0)
      return false;

    tableName = std::string(base, leftBracket-base);

#if defined(__APPLE__)
  boost::to_upper(tableName);
#endif

    ptr = leftBracket+1;
    if (ptr == dotdot)
      startRow = "";
    else {
      startRow = std::string(ptr, dotdot-ptr);
      boost::trim(startRow);
      boost::trim_if(startRow, boost::is_any_of("'\""));
      if (startRow.rfind("??",startRow.length()) == (startRow.length()-2)) {
	size_t offset = startRow.length() - 2;
	startRow.replace(offset, 2, 2, (char )0xff);
      }
    }

    if (ptr == rightBracket)
      return false;
    ptr = dotdot+2;
    endRow = std::string(ptr, rightBracket-ptr);
    boost::trim(endRow);
    boost::trim_if(endRow, boost::is_any_of("'\""));
    if (endRow.rfind("??",endRow.length()) == (endRow.length()-2)) {
      size_t offset = endRow.length() - 2;
      endRow.replace(offset, 2, 2, (char )0xff);
    }

    return true;
  }

}
