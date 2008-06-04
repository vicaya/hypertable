/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#ifndef HYPERTABLE_RANGEUPDATEBARRIER_H
#define HYPERTABLE_RANGEUPDATEBARRIER_H

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>


namespace Hypertable {

  class RangeUpdateBarrier {
  public:

    RangeUpdateBarrier() : m_hold_updates(false), m_update_counter(0) {
    }

    /**
     */
    void enter() {
      boost::mutex::scoped_lock lock(m_mutex);
      while (m_hold_updates)
	m_updates_unblocked_cond.wait(lock);
      m_update_counter++;
    }

    /**
     */
    void exit() {
      boost::mutex::scoped_lock lock(m_mutex);
      m_update_counter--;
      if (m_hold_updates && m_update_counter == 0)
	m_updates_quiesced_cond.notify_one();
    }

    /**
     */
    void put_up() {
      boost::mutex::scoped_lock lock(m_mutex);
      m_hold_updates = true;
      while (m_update_counter > 0)
	m_updates_quiesced_cond.wait(lock);
    }

    /**
     */
    void take_down() {
      boost::mutex::scoped_lock lock(m_mutex);
      m_hold_updates = false;
      m_updates_unblocked_cond.notify_all();
    }

    class ScopedActivator {
    public:
      ScopedActivator(RangeUpdateBarrier &barrier) : m_barrier(barrier) {
	m_barrier.put_up();
      }
      ~ScopedActivator() {
	m_barrier.take_down();
      }
    private:
      RangeUpdateBarrier &m_barrier;
    };

  private:
    boost::mutex m_mutex;
    boost::condition m_updates_unblocked_cond;
    boost::condition m_updates_quiesced_cond;
    bool             m_hold_updates;
    uint32_t         m_update_counter;

  };

}

#endif // HYPERTABLE_RANGEUPDATEBARRIER_H
