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
#ifndef HYPERTABLE_POLLTIMEOUT_H
#define HYPERTABLE_POLLTIMEOUT_H

// remove
#include <iostream>

#include <boost/thread/xtime.hpp>

#include <cassert>

namespace Hypertable {

  class PollTimeout {

  public:

    PollTimeout() : ts_ptr(0), duration_millis(-1) { return; }

    void set(boost::xtime &now, boost::xtime &expire) {
      assert((xtime_cmp(now , expire) <= 0));
      if (now.sec == expire.sec) {
	duration_ts.tv_sec = 0;
	duration_ts.tv_nsec = expire.nsec - now.nsec;
      }
      else {
	uint64_t nanos = expire.nsec + (1000000000LL - now.nsec);
	duration_ts.tv_sec = ((expire.sec - now.sec) - 1) + (nanos / 1000000000LL);
	duration_ts.tv_nsec = nanos % 1000000000LL;
      }
      ts_ptr = &duration_ts;
      duration_millis = (int)((duration_ts.tv_sec * 1000) + (duration_ts.tv_nsec / 1000000));
      assert(duration_millis >= 0);
    }

    void set_indefinite() {
      ts_ptr = 0;
      duration_millis = -1;
    }

    int get_millis() { return duration_millis; }

    struct timespec *get_timespec() { return ts_ptr; }

  private:

    struct timespec *ts_ptr;
    struct timespec duration_ts;
    int duration_millis;
  };

}

#endif // HYPERTABLE_POLLTIMEOUT_H
