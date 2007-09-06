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

#include "ParseRangeSpec.h"

namespace hypertable {

  bool ParseRangeSpec(std::string &spec, std::string &tableName, std::string &startRow, std::string &endRow) {
    const char *base = spec.c_str();
    const char *ptr, *leftBracket, *colon, *rightBracket;

    if ((leftBracket = strchr(base, '[')) == 0)
      return false;

    if ((colon = strchr(leftBracket, ':')) == 0)
      return false;

    if ((rightBracket = strchr(colon, ']')) == 0)
      return false;

    tableName = std::string(base, leftBracket-base);

    ptr = leftBracket+1;
    if (ptr == colon)
      startRow = "";
    else
      startRow = std::string(ptr, colon-ptr);

    ptr = colon+1;
    if (ptr == rightBracket)
      endRow = "";
    else
      endRow = std::string(ptr, rightBracket-ptr);

    return true;
  }

}
