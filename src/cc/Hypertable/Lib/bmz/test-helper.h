/**
 * Copyright (C) 2007 Luke Lu (Zvents, Inc.)
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

#ifndef HT_SIMPLE_TEST_HELPER_H
#define HT_SIMPLE_TEST_HELPER_H

#include <sys/time.h>

#define HT_MEASURE(_t_, _code_) do { \
  double t0 = ht_time_d(); _code_; _t_ = ht_time_d() - t0; \
} while (0)

static inline double 
ht_time_d() {
  struct timeval tv;
  if (gettimeofday(&tv, NULL)) {
    perror(__FUNCTION__);
  }
  return (double)(tv.tv_sec) + (double)(tv.tv_usec) / 1e6;
}

#define HT_CHECK(e) \
    ((void)((e) ? 0 : HT_CHECK_FAIL(#e)))

#define HT_CHECK_FAIL(e) \
    ((void)printf("%s:%u: %s: bad assertion: `%s'\n", \
                  __FILE__, __LINE__, __FUNCTION__, e), abort(), 0)

#endif /* HT_SIMPLE_TEST_HELPER_H */
