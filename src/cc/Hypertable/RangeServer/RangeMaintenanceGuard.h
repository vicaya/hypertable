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

#ifndef HYPERTABLE_RANGEMAINTENANCEGUARD_H
#define HYPERTABLE_RANGEMAINTENANCEGUARD_H

#include <boost/thread/condition.hpp>

#include "Common/Mutex.h"

namespace Hypertable {

  class RangeMaintenanceGuard {
  public:

    RangeMaintenanceGuard() : m_in_progress(false) {}

    void activate() {
      ScopedLock lock(m_mutex);
      if (m_in_progress)
        HT_THROW(Error::RANGESERVER_RANGE_BUSY, "");
      m_in_progress = true;
    }

    void deactivate() {
      ScopedLock lock(m_mutex);
      m_in_progress = false;
      m_cond.notify_all();
    }

    void wait_for_complete() {
      ScopedLock lock(m_mutex);
      while (m_in_progress)
        m_cond.wait(lock);
    }

    bool in_progress() {
      ScopedLock lock(m_mutex);
      return m_in_progress;
    }

    class Activator {
    public:
      Activator(RangeMaintenanceGuard &guard) : m_guard(&guard) {
        m_guard->activate();
      }
      ~Activator() {
        m_guard->deactivate();
      }
    private:
      RangeMaintenanceGuard *m_guard;
    };

  private:
    Mutex m_mutex;
    boost::condition m_cond;
    bool m_in_progress;
  };

}


#endif // HYPERTABLE_RANGEMAINTENANCEGUARD_H
