/** -*- c++ -*-
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

#ifndef HYPERTABLE_TIMER_H
#define HYPERTABLE_TIMER_H

#include <cstring>

#include <boost/thread/xtime.hpp>

namespace Hypertable {

  /**
   */
  class Timer {
  public:

    Timer(double value, bool start_timer=false) : m_running(false), m_remaining(value) {
      if (start_timer)
        start();
    }

    void start() {
      if (!m_running) {
        boost::xtime_get(&start_time, boost::TIME_UTC);
        m_running = true;
      }
    }

    void stop() {
      boost::xtime stop_time;
      boost::xtime_get(&stop_time, boost::TIME_UTC);
      if (start_time.sec == stop_time.sec)
        m_remaining -= (double)(stop_time.nsec - start_time.nsec) / 1000000000.0;
      else {
        m_remaining -= stop_time.sec - start_time.sec;
        m_remaining -= ((1000000000.0 - (double)start_time.nsec) + (double)stop_time.nsec) / 1000000000.0;
      }
      m_running = false;
    }

    double remaining() {
      if (m_running) { stop(); start(); }
      return (m_remaining < 0.0) ? 0.0 : m_remaining;
    }

    bool expired() { return remaining() <= 0.0; }

    bool is_running() { return m_running; }

  private:
    bool m_running;
    boost::xtime start_time;
    double m_remaining;
  };

}

#endif // HYPERTABLE_TIMER_H
