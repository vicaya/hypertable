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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERSPACE_SESSIONDATA_H
#define HYPERSPACE_SESSIONDATA_H

#include <list>
#include <set>

#include <boost/thread/mutex.hpp>
#include <boost/thread/xtime.hpp>

#include "Common/ReferenceCount.h"

#include "Notification.h"

using namespace Hypertable;

namespace Hyperspace {

  class SessionData : public ReferenceCount {
  public:
    SessionData(struct sockaddr_in &_addr, uint32_t leaseInterval, uint64_t _id) : addr(_addr), m_lease_interval(leaseInterval), id(_id), expired(false) {
      boost::xtime_get(&expireTime, boost::TIME_UTC);
      expireTime.sec += leaseInterval;
      return;
    }

    void add_notification(Notification *notification) {
      boost::mutex::scoped_lock lock(mutex);
      if (expired) {
	notification->eventPtr->decrement_notification_count();
	delete notification;
      }
      else
	notifications.push_back(notification);
    }

    void purge_notifications(uint64_t eventId) {
      boost::mutex::scoped_lock lock(mutex);
      list<Notification *>::iterator iter = notifications.begin();
      while (iter != notifications.end()) {
	if ((*iter)->eventPtr->get_id() <= eventId) {
	  (*iter)->eventPtr->decrement_notification_count();
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
      if (xtime_cmp(expireTime, now) < 0) {
	expired = true;
	list<Notification *>::iterator iter = notifications.begin();
	while (iter != notifications.end()) {
	  (*iter)->eventPtr->decrement_notification_count();
	  delete *iter;
	  iter = notifications.erase(iter);
	}
	return false;
      }
      memcpy(&expireTime, &now, sizeof(boost::xtime));
      expireTime.sec += m_lease_interval;
      return true;
    }

    bool is_expired(boost::xtime &now) {
      boost::mutex::scoped_lock lock(mutex);
      return (xtime_cmp(expireTime, now) < 0) ? true : false;
    }

    void expire() {
      boost::mutex::scoped_lock lock(mutex);
      if (expired)
	return;
      expired = true;
      list<Notification *>::iterator iter = notifications.begin();
      while (iter != notifications.end()) {
	(*iter)->eventPtr->decrement_notification_count();
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
    boost::xtime expireTime;
    set<uint64_t> handles;
    list<Notification *> notifications;
  };

  typedef boost::intrusive_ptr<SessionData> SessionDataPtr;

  struct ltSessionData {
    bool operator()(const SessionDataPtr &sd1, const SessionDataPtr &sd2) const {
      return xtime_cmp(sd1->expireTime, sd2->expireTime) >= 0;
    }
  };

}

#endif // HYPERSPACE_SESSIONDATA_H
