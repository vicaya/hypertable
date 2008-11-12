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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Compat.h"

#include <iomanip>

#include "Time.h"
#include "Mutex.h"


using namespace std;
using namespace Hypertable;

uint64_t
Hypertable::get_ts64() {
  static Mutex mutex;
  ScopedLock lock(mutex);
  HiResTime now;
  return ((uint64_t)now.sec * 1000000000LL) + (uint64_t)now.nsec;
}

bool Hypertable::xtime_add_millis(boost::xtime &xt, uint32_t millis) {
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

bool Hypertable::xtime_sub_millis(boost::xtime &xt, uint32_t millis) {
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

std::ostream &Hypertable::hires_ts(std::ostream &out) {
  HiResTime now;
  out << now.sec <<'.'<< setw(9) << setfill('0') << now.nsec;
  return out;
}
