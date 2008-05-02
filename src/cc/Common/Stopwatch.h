/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_STOPWATCH_H
#define HYPERTABLE_STOPWATCH_H

#include <cstring>

#include <boost/thread/xtime.hpp>

namespace Hypertable {

  /**
   * 
   */
  class Stopwatch {
  public:

    Stopwatch() { memset(&elapsed_time, 0, sizeof(elapsed_time)); }

    void start() { boost::xtime_get(&start_time, boost::TIME_UTC); }

    void stop() {
      boost::xtime stop_time;
      boost::xtime_get(&stop_time, boost::TIME_UTC);
      if (start_time.sec == stop_time.sec)
	elapsed_time.nsec += stop_time.nsec - start_time.nsec;
      else {
	elapsed_time.sec += stop_time.sec - start_time.sec;
	elapsed_time.nsec += (1000000000L - start_time.nsec) + stop_time.nsec;
	if (elapsed_time.nsec > 1000000000L) {
	  elapsed_time.sec += elapsed_time.nsec / 1000000000L;
	  elapsed_time.nsec %= 1000000000L;
	}
      }
    }

    void reset() { memset(&elapsed_time, 0, sizeof(elapsed_time)); }

    double elapsed() const { return (double)elapsed_time.sec + ((double)elapsed_time.nsec / 1000000000.0); }

    boost::xtime &elapsed() { return elapsed_time; }

  private:
    boost::xtime start_time;
    boost::xtime elapsed_time;
  };

}

#endif // HYPERTABLE_STOPWATCH_H
