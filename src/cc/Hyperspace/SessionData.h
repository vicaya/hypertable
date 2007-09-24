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

using namespace hypertable;

namespace Hyperspace {

  class SessionData : public ReferenceCount {
  public:
    SessionData(struct sockaddr_in &_addr, uint32_t leaseInterval, uint64_t _id) : addr(_addr), mLeaseInterval(leaseInterval), id(_id) {
      boost::xtime_get(&expireTime, boost::TIME_UTC);
      expireTime.sec += leaseInterval;
      return;
    }

    void RenewLease() {
      boost::xtime_get(&expireTime, boost::TIME_UTC);      
      expireTime.sec + mLeaseInterval;
    }

    void AddNotification(Notification *notification) {
      boost::mutex::scoped_lock slock(mutex);
      notifications.push_back(notification);
    }

    void PurgeNotifications(uint64_t eventId) {
      boost::mutex::scoped_lock slock(mutex);
      list<Notification *>::iterator iter = notifications.begin();
      while (iter != notifications.end()) {
	if ((*iter)->event->GetId() <= eventId) {
	  (*iter)->event->DecrementNotificationCount();
	  delete *iter;
	  iter = notifications.erase(iter);
	}
	else
	  iter++;
      }
    }

    boost::mutex mutex;
    struct sockaddr_in addr;
    uint32_t mLeaseInterval;
    uint64_t id;
    boost::xtime expireTime;
    set<uint64_t> handles;
    list<Notification *> notifications;
  };

  typedef boost::intrusive_ptr<SessionData> SessionDataPtr;

}

#endif // HYPERSPACE_SESSIONDATA_H
