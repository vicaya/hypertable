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

#ifndef HYPERTABLE_MAINTENANCETASK_H
#define HYPERTABLE_MAINTENANCETASK_H

#include <boost/thread/xtime.hpp>

namespace Hypertable {

  class MaintenanceTask {
  public:
    MaintenanceTask(boost::xtime start_time_) : start_time(start_time_), m_retry(false) { return; }
    MaintenanceTask(boost::xtime start_time_, time_t retry_delay_seconds) : start_time(start_time_), m_retry(true), m_retry_delay_seconds(retry_delay_seconds) { return; }
    MaintenanceTask() : m_retry(false) { boost::xtime_get(&start_time, boost::TIME_UTC); return; }
    MaintenanceTask(time_t retry_delay_seconds) : m_retry(true), m_retry_delay_seconds(retry_delay_seconds) { boost::xtime_get(&start_time, boost::TIME_UTC); return; }
    virtual ~MaintenanceTask() { return; }
    virtual void execute() = 0;
    boost::xtime start_time;
  private:
    bool m_retry;
    time_t m_retry_delay_seconds;
  };

}

#endif // HYPERTABLE_MAINTENANCETASK_H
