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

#ifndef HYPERSPACE_EVENT_H
#define HYPERSPACE_EVENT_H

#include <iostream>
#include <string>

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include "Common/ReferenceCount.h"

#include "HandleCallback.h"

namespace Hyperspace {

  class Event : public hypertable::ReferenceCount {
  public:
    Event(uint64_t id, int type, std::string name) : mId(id), mType(type), mName(name), mNotificationCount(0) { return; }
    uint64_t GetId() { return mId; }
    int GetType() { return mType; }
    std::string &GetName() { return mName; }
    void IncrementNotificationCount() {
      boost::mutex::scoped_lock lock(mMutex);
      mNotificationCount++;
    }
    void DecrementNotificationCount() {
      boost::mutex::scoped_lock lock(mMutex);
      mNotificationCount--;
      if (mNotificationCount == 0)
	mCond.notify_all();
    }
    void WaitForNotifications() {
      boost::mutex::scoped_lock lock(mMutex);
      if (mNotificationCount != 0)
	mCond.wait(lock);
    }

  private:
    boost::mutex      mMutex;
    boost::condition  mCond;
    uint64_t mId;
    int mType;
    std::string mName;
    uint32_t mNotificationCount;
  };
  typedef boost::intrusive_ptr<Event> HyperspaceEventPtr;

}

#endif // HYPERSPACE_EVENT_H
