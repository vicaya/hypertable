/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_BARRIER_H
#define HYPERTABLE_BARRIER_H

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include "Mutex.h"

namespace Hypertable {

  class Barrier {
  public:

    Barrier() : m_hold(false), m_counter(0) {
    }

    /**
     */
    void enter() {
      ScopedLock lock(m_mutex);
      while (m_hold)
        m_unblocked_cond.wait(lock);
      m_counter++;
    }

    /**
     */
    void exit() {
      ScopedLock lock(m_mutex);
      m_counter--;
      if (m_hold && m_counter == 0)
        m_quiesced_cond.notify_one();
    }

    /**
     */
    void put_up() {
      ScopedLock lock(m_mutex);
      m_hold = true;
      while (m_counter > 0)
        m_quiesced_cond.wait(lock);
    }

    /**
     */
    void take_down() {
      ScopedLock lock(m_mutex);
      m_hold = false;
      m_unblocked_cond.notify_all();
    }

    class ScopedActivator {
    public:
      ScopedActivator(Barrier &barrier) : m_barrier(barrier) {
        m_barrier.put_up();
      }
      ~ScopedActivator() {
        m_barrier.take_down();
      }
    private:
      Barrier &m_barrier;
    };

  private:
    Mutex            m_mutex;
    boost::condition m_unblocked_cond;
    boost::condition m_quiesced_cond;
    bool             m_hold;
    uint32_t         m_counter;

  };

}

#endif // HYPERTABLE_BARRIER_H
