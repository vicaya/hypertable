/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERSPACE_SESSIONDATA_H
#define HYPERSPACE_SESSIONDATA_H

#include <list>
#include <set>

#include <boost/thread/mutex.hpp>
#include <boost/thread/xtime.hpp>

#include "Common/ReferenceCount.h"

#include "Notification.h"


namespace Hyperspace {

  using namespace Hypertable;

  class SessionData : public ReferenceCount {
  public:
    SessionData(struct sockaddr_in &_addr, uint32_t lease_interval, uint64_t _id) : addr(_addr), m_lease_interval(lease_interval), id(_id), expired(false) {
      boost::xtime_get(&expire_time, boost::TIME_UTC);
      expire_time.sec += lease_interval;
      return;
    }

    void add_notification(Notification *notification) {
      boost::mutex::scoped_lock lock(mutex);
      if (expired) {
        notification->event_ptr->decrement_notification_count();
        delete notification;
      }
      else
        notifications.push_back(notification);
    }

    void purge_notifications(uint64_t event_id) {
      boost::mutex::scoped_lock lock(mutex);
      std::list<Notification *>::iterator iter = notifications.begin();
      while (iter != notifications.end()) {
        if ((*iter)->event_ptr->get_id() <= event_id) {
          (*iter)->event_ptr->decrement_notification_count();
          delete *iter;
          iter = notifications.erase(iter);
        }
        else
          iter++;
      }
    }

    bool renew_lease() {
      boost::mutex::scoped_lock lock(mutex);
      boost::xtime now;
      boost::xtime_get(&now, boost::TIME_UTC);
      if (xtime_cmp(expire_time, now) < 0) {
        expired = true;
        std::list<Notification *>::iterator iter = notifications.begin();
        while (iter != notifications.end()) {
          (*iter)->event_ptr->decrement_notification_count();
          delete *iter;
          iter = notifications.erase(iter);
        }
        return false;
      }
      memcpy(&expire_time, &now, sizeof(boost::xtime));
      expire_time.sec += m_lease_interval;
      return true;
    }

    bool is_expired(boost::xtime &now) {
      boost::mutex::scoped_lock lock(mutex);
      return (xtime_cmp(expire_time, now) < 0) ? true : false;
    }

    void expire() {
      boost::mutex::scoped_lock lock(mutex);
      if (expired)
        return;
      expired = true;
      std::list<Notification *>::iterator iter = notifications.begin();
      while (iter != notifications.end()) {
        (*iter)->event_ptr->decrement_notification_count();
        delete *iter;
        iter = notifications.erase(iter);
      }
    }

    void add_handle(uint64_t handle) {
      boost::mutex::scoped_lock lock(mutex);
      handles.insert(handle);
    }

    boost::mutex mutex;
    struct sockaddr_in addr;
    uint32_t m_lease_interval;
    uint64_t id;
    bool expired;
    boost::xtime expire_time;
    std::set<uint64_t> handles;
    std::list<Notification *> notifications;
  };

  typedef boost::intrusive_ptr<SessionData> SessionDataPtr;

  struct LtSessionData {
    bool operator()(const SessionDataPtr &sd1, const SessionDataPtr &sd2) const {
      return xtime_cmp(sd1->expire_time, sd2->expire_time) >= 0;
    }
  };

}

#endif // HYPERSPACE_SESSIONDATA_H
