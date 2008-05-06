/**
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef HYPERTABLE_STRING_H
#define HYPERTABLE_STRING_H

#include <string>

namespace Hypertable {
  /**
   * We might want to use something better later, as std::string always
   * causes a heap allocation, and is lacking in functionalities
   * cf. http://www.and.org/vstr/comparison
   */
  typedef std::string String;
  typedef long unsigned int Lu;         // shortcut for printf format
  typedef long long unsigned int Llu;   // ditto

  /** 
   * return a String using printf like format facilities
   * vanilla snprintf is about 1.5x faster than this, which is about:
   *   10x faster than boost::format;
   *   1.5x faster than std::string append (operator+=);
   *   3.5x faster than std::string operator+;
   */
  String format(const char *fmt, ...) __attribute__((format (printf, 1, 2)));
}

#endif // HYPERTABLE_STRING_H
