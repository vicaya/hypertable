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

#include "MaintenanceFlag.h"
#include "Range.h"

namespace Hypertable {

  class MaintenanceTask {
  public:

    MaintenanceTask(boost::xtime &stime, RangePtr &range, const String &desc)
      : start_time(stime), priority(0), m_range(range), m_retry(false),
        m_description(desc) { }

    MaintenanceTask(const String &desc) : priority(0), m_retry(false),
                                          m_description(desc) {
      boost::xtime_get(&start_time, boost::TIME_UTC);
    }

    virtual ~MaintenanceTask() { }

    virtual void execute() = 0;

    String &description() { return m_description; }

    bool retry() { return m_retry; }
    void set_retry(bool retry) { m_retry = retry; }

    uint32_t get_retry_delay() { return m_retry_delay_millis; }
    void set_retry_delay(uint32_t delay) { m_retry_delay_millis = delay; }

    int get_priority() { return priority; }
    void set_priority(int p) { priority = p; }

    Range *get_range() { return m_range.get(); }

    void add_subtask(const void *obj, int flags) {
      m_map[obj] = flags;
    }

    boost::xtime start_time;
    int priority;

  protected:
    RangePtr m_range;
    MaintenanceFlag::Map m_map;

  private:
    bool m_retry;
    uint32_t m_retry_delay_millis;
    String m_description;
  };

}

#endif // HYPERTABLE_MAINTENANCETASK_H
