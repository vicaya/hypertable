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

#ifndef HYPERSPACE_TIMERHANDLER_H
#define HYPERSPACE_TIMERHANDLER_H

#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/xtime.hpp>

#include "Common/Mutex.h"

#include "RangeServer.h"
#include "TimerInterface.h"

namespace Hypertable {

  /**
   */
  class TimerHandler : public TimerInterface {

  public:
    TimerHandler(Comm *comm, RangeServer *range_server);
    virtual void handle(Hypertable::EventPtr &event_ptr);
    virtual void schedule_maintenance();
    virtual void complete_maintenance_notify();
    virtual bool low_memory() { return m_app_queue_paused; }

  private:
    Comm         *m_comm;
    RangeServer  *m_range_server;
    ApplicationQueuePtr m_app_queue;
    int32_t       m_timer_interval;
    int32_t       m_current_interval;
    int64_t       m_last_low_memory_maintenance;
    bool          m_urgent_maintenance_scheduled;
    bool          m_app_queue_paused;
    boost::xtime  m_last_maintenance;
    bool          m_maintenance_outstanding;
  };
  typedef boost::intrusive_ptr<TimerHandler> TimerHandlerPtr;
}

#endif // HYPERSPACE_TIMERHANDLER_H

