/**
 * Copyright (C) 2009 Luke Lu (Zvents, Inc.)
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

#include "Compat.h"

#include <time.h>
#include <iomanip>

#include "Time.h"
#include "Mutex.h"


using namespace std;

namespace Hypertable {

int64_t get_ts64() {
  HiResTime now;
  return ((int64_t)now.sec * 1000000000LL) + (int64_t)now.nsec;
}

bool xtime_add_millis(boost::xtime &xt, uint32_t millis) {
  uint64_t nsec = (uint64_t)xt.nsec + ((uint64_t)millis * 1000000LL);
  if (nsec > 1000000000LL) {
    uint32_t new_secs = xt.sec + (uint32_t)(nsec / 1000000000LL);
    if (new_secs < xt.sec)
      return false;
    xt.sec = new_secs;
    xt.nsec = (uint32_t)(nsec % 1000000000LL);
  }
  else
    xt.nsec = nsec;
  return true;
}

bool xtime_sub_millis(boost::xtime &xt, uint32_t millis) {
  uint64_t nsec = (uint64_t)millis * 1000000LL;

  if (nsec <= (uint64_t)xt.nsec)
    xt.nsec -= (uint32_t)nsec;
  else {
    uint32_t secs = millis / 1000;
    uint32_t rem = (uint32_t)(nsec % 1000000000LL);
    if (rem <= (uint32_t)xt.nsec) {
      if (secs < xt.sec)
        return false;
      xt.sec -= secs;
      xt.nsec -= rem;
    }
    else {
      secs++;
      if (secs < xt.sec)
        return false;
      xt.sec -= secs;
      xt.nsec = 1000000000LL - (rem % xt.nsec);
    }
  }
  return true;
}

int64_t xtime_diff_millis(boost::xtime &early_xt, boost::xtime &late_xt) {
  int64_t total_millis = 0;

  if (early_xt.sec > late_xt.sec)
    return 0;

  if (early_xt.sec < late_xt.sec) {
    total_millis = (late_xt.sec - (early_xt.sec+1)) * 1000;
    total_millis += 1000 - (early_xt.nsec / 1000000);
    total_millis += late_xt.nsec / 1000000;
  }
  else if (early_xt.nsec > late_xt.nsec)
    return 0;
  else
    total_millis = (late_xt.nsec - early_xt.nsec) / 1000000;

  return total_millis;
}

std::ostream &hires_ts(std::ostream &out) {
  HiResTime now;
  return out << now.sec <<'.'<< setw(9) << setfill('0') << now.nsec;
}

std::ostream &hires_ts_date(std::ostream &out) {
  tm tv;
  HiResTime now;
  time_t s = now.sec; // using const time_t * is not convenient
  gmtime_r(&s, &tv);
  return out << tv.tm_year + 1900 << '-'
             << right << setw(2) << setfill('0') << tv.tm_mon + 1 << '-'
             << right << setw(2) << setfill('0') << tv.tm_mday
             << ' '
             << right << setw(2) << setfill('0') << tv.tm_hour << ':'
             << right << setw(2) << setfill('0') << tv.tm_min << ':'
             << right << setw(2) << setfill('0') << tv.tm_sec << '.'
             << right << setw(9) << setfill('0') << now.nsec;
}

#if defined(__sun__)
  time_t timegm(struct tm *t) {
    time_t tl, tb;
    struct tm *tg;

    tl = mktime (t);
    if (tl == -1)
      {
	t->tm_hour--;
	tl = mktime (t);
	if (tl == -1)
	  return -1; /* can't deal with output from strptime */
	tl += 3600;
      }
    tg = gmtime (&tl);
    tg->tm_isdst = 0;
    tb = mktime (tg);
    if (tb == -1)
      {
	tg->tm_hour--;
	tb = mktime (tg);
	if (tb == -1)
	  return -1; /* can't deal with output from gmtime */
	tb += 3600;
      }
    return (tl - (tb - tl));
  }
#endif

} // namespace Hypertable
