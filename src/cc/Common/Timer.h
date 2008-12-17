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

#include "Logger.h"

namespace Hypertable {

  /**
   */
  class Timer {
  public:

    Timer(uint32_t millis, bool start_timer=false)
      : m_running(false), m_duration(millis), m_remaining(millis) {
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
      uint32_t adjustment;

      if (start_time.sec == stop_time.sec) {
        adjustment = (stop_time.nsec - start_time.nsec) / 1000000;
        m_remaining = (adjustment < m_remaining) ? m_remaining - adjustment : 0;
      }
      else {
        adjustment = ((stop_time.sec - start_time.sec) - 1) * 1000;
        m_remaining = (adjustment < m_remaining) ? m_remaining - adjustment : 0;
        adjustment = ((1000000000 - start_time.nsec) + stop_time.nsec) / 1000000;
        m_remaining = (adjustment < m_remaining) ? m_remaining - adjustment : 0;
      }
      m_running = false;
    }

    uint32_t remaining() {
      if (m_running) { stop(); start(); }
      return m_remaining;
    }

    bool expired() { return remaining() == 0; }

    bool is_running() { return m_running; }

  private:
    bool m_running;
    boost::xtime start_time;
    uint32_t m_duration;
    uint32_t m_remaining;
  };

}

#endif // HYPERTABLE_TIMER_H
