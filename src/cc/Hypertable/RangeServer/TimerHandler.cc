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
#include "Common/Compat.h"
#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/StringExt.h"
#include "Common/System.h"
#include "Common/Time.h"

#include <algorithm>

#include "RequestHandlerDoMaintenance.h"
#include "TimerHandler.h"

using namespace Hypertable;
using namespace Hypertable::Config;

/**
 *
 */
TimerHandler::TimerHandler(Comm *comm, RangeServer *range_server)
  : m_comm(comm), m_range_server(range_server),
    m_urgent_maintenance_scheduled(false), m_app_queue_paused(false),
    m_maintenance_outstanding(false) {
  int error;
  int32_t maintenance_interval;

  m_timer_interval = get_i32("Hypertable.RangeServer.Timer.Interval");
  maintenance_interval = get_i32("Hypertable.RangeServer.Maintenance.Interval");

  if (m_timer_interval > (maintenance_interval+10)) {
    m_timer_interval = maintenance_interval + 10;
    HT_INFOF("Reducing timer interval to %d to support maintenance interval %d",
             m_timer_interval, maintenance_interval);
  }

  m_current_interval = m_timer_interval;

  boost::xtime_get(&m_last_maintenance, TIME_UTC);

  m_app_queue = m_range_server->get_application_queue();

  range_server->register_timer(this);

  if ((error = m_comm->set_timer(0, this)) != Error::OK)
    HT_FATALF("Problem setting timer - %s", Error::get_text(error));

  return;
}


void TimerHandler::schedule_maintenance() {
  ScopedLock lock(m_mutex);

  if (!m_urgent_maintenance_scheduled && !m_maintenance_outstanding) {
    boost::xtime now;
    boost::xtime_get(&now, TIME_UTC);
    uint64_t elapsed = xtime_diff_millis(m_last_maintenance, now);
    int error;
    uint32_t millis = (elapsed < 1000) ? 1000 - elapsed : 0;
    HT_INFOF("Scheduling urgent maintenance for %u millis in the future", millis);
    if ((error = m_comm->set_timer(millis, this)) != Error::OK)
      HT_FATALF("Problem setting timer - %s", Error::get_text(error));
    m_urgent_maintenance_scheduled = true;
  }
  return;
}


void TimerHandler::complete_maintenance_notify() {
  ScopedLock lock(m_mutex);
  m_maintenance_outstanding = false;
  boost::xtime_get(&m_last_maintenance, TIME_UTC);
}



/**
 *
 */
void TimerHandler::handle(Hypertable::EventPtr &event_ptr) {
  ScopedLock lock(m_mutex);
  int error;

  if (m_shutdown) {
    HT_INFO("TimerHandler shutting down.");
    m_shutdown_complete = true;
    m_shutdown_cond.notify_all();
    return;
  }

  int64_t memory_used = Global::memory_tracker->balance();

  if (memory_used > Global::memory_limit) {
    m_current_interval = 500;
    if (!m_app_queue_paused) {
      HT_INFO("Pausing application queue due to low memory condition");
      m_app_queue_paused = true;
      m_app_queue->stop();
    }
  }
  else {
    if (m_app_queue_paused) {
      HT_INFO("Restarting application queue");
      m_app_queue->start();
      m_app_queue_paused = false;
    }
    if (m_current_interval < m_timer_interval)
      m_current_interval = std::min(m_current_interval*2, m_timer_interval);
  }

  try {

    if (event_ptr->type == Hypertable::Event::TIMER) {

      if (!m_maintenance_outstanding) {
        m_app_queue->add( new RequestHandlerDoMaintenance(m_comm, m_range_server) );
        m_maintenance_outstanding = true;
      }

      if (m_urgent_maintenance_scheduled)
        m_urgent_maintenance_scheduled = false;
      else {
        //HT_INFOF("About to reset timer to %u millis in the future", m_current_interval);
        if ((error = m_comm->set_timer(m_current_interval, this)) != Error::OK)
          HT_FATALF("Problem setting timer - %s", Error::get_text(error));
      }
    }
    else
      HT_ERRORF("Unexpected event - %s", event_ptr->to_str().c_str());
  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
}
