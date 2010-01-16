/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERSPACE_TIMERINTERFACE_H
#define HYPERSPACE_TIMERINTERFACE_H

namespace Hypertable {

  /**
   */
  class TimerInterface : public DispatchHandler {
  public:
    TimerInterface() : m_shutdown(false), m_shutdown_complete(false) {};
    virtual void handle(Hypertable::EventPtr &event_ptr) = 0;
    virtual void schedule_maintenance() = 0;
    virtual void complete_maintenance_notify() = 0;
    virtual bool low_memory() = 0;

    void shutdown() { m_shutdown = true; }

    void wait_for_shutdown() {
      ScopedLock lock(m_mutex);
      while (!m_shutdown_complete)
        m_shutdown_cond.wait(lock);
    }

  protected:
    Mutex            m_mutex;
    boost::condition m_shutdown_cond;
    bool             m_shutdown;
    bool             m_shutdown_complete;
  };

}


#endif // HYPERSPACE_TIMERINTERFACE_H
