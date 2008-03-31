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

#include <cstdarg>

#include "String.h"

namespace Hypertable {

String format(const char *fmt, ...) {
  char buf[512];
  int n, size = sizeof(buf);
  char *p = buf;
  va_list ap;

  do {
    va_start(ap, fmt);
    n = vsnprintf(p, size, fmt, ap);
    va_end(ap);
    if (n > -1 && n < size) return p; // worked!
    if (n > -1)         // glibc 2.1
      size = n + 1;     //   exactly what's needed
    else                // glibc 2.0
      size *= 2;        //   double the size and try again
    if ((p = (char *) malloc(size)) == NULL) return "Out of memory";
  } while (true);
}

} // namespace Hypertable
