/** -*- C++ -*-
 * Copyright (C) 2008  Luke Lu (Zvents, Inc.)
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

#ifndef HYPERTABLE_TIMEINLINE_H
#define HYPERTABLE_TIMEINLINE_H

#include <stdexcept>

#include "Time.h"

// Some header only code (no link/runtime library dependency)
namespace Hypertable {

#ifndef HT_DELIM_CHECK
# define HT_DELIM_CHECK(_l_, _c_) do { \
    if ((_l_) != (_c_)) \
      throw std::runtime_error("expected " #_l_ " to be " #_c_); \
  } while (0)
#endif

#ifndef HT_RANGE_CHECK
# define HT_RANGE_CHECK(_v_, _min_, _max_) do { \
    if ((_v_) < (_min_) || (_v_) > (_max_)) \
      throw std::range_error(#_v_ " must be between " #_min_ " and " #_max_); \
  } while (0)
#endif

// Parse timestamp format in YYYY-mm-dd[ HH:MM[:SS[.SS|:NS]]]
inline int64_t
parse_ts(const char *ts) {
  const int64_t G = 1000000000LL;
  tm tv;
  char *last;
  double sec=0;
  int64_t ns=0;

  memset(&tv, 0, sizeof(tm));
  tv.tm_year = strtol(ts, &last, 10) - 1900;
  HT_DELIM_CHECK(*last, '-');
  tv.tm_mon = strtol(last + 1, &last, 10) - 1;
  HT_RANGE_CHECK(tv.tm_mon, 0, 11);
  HT_DELIM_CHECK(*last, '-');
  tv.tm_mday = strtol(last + 1, &last, 10);
  HT_RANGE_CHECK(tv.tm_mday, 1, 31);

  if (*last == 0)
    return timegm(&tv) * G;

  HT_DELIM_CHECK(*last, ' ');
  tv.tm_hour = strtol(last + 1, &last, 10) - 1;
  HT_RANGE_CHECK(tv.tm_hour, 0, 23);
  HT_DELIM_CHECK(*last, ':');
  tv.tm_min = strtol(last + 1, &last, 10) - 1;
  HT_RANGE_CHECK(tv.tm_min, 0, 59);

  if (*last == 0)
    return timegm(&tv) * G;

  HT_DELIM_CHECK(*last, ':');

  sec = strtod(last+1, &last) - 1;
  tv.tm_sec = 0;
  HT_RANGE_CHECK(sec, 0, 60);
  // integer nanoseconds
  if (*last == ':') {
    ns = strtol(last+1, &last, 10);
  }

  return (int64_t)(timegm(&tv) * G + sec * G + ns);
}

} // namespace Hypertable

#endif /* HYPERTABLE_TIMEINLINE_H */
