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
#include "AsyncComm/CommBuf.h"
#include "AsyncComm/Serialization.h"

#include "HandleCallback.h"

namespace Hyperspace {

  class Event : public hypertable::ReferenceCount {
  public:
    Event(uint64_t id, uint32_t mask) : mId(id), mMask(mask), mNotificationCount(0) { return; }
    virtual ~Event() { return; }

    uint64_t GetId() { return mId; }

    uint32_t GetMask() { return mMask; }

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

    virtual uint32_t EncodedLength() = 0;
    virtual void Encode(hypertable::CommBuf *cbuf) = 0;

  protected:
    boost::mutex      mMutex;
    boost::condition  mCond;
    uint64_t mId;
    uint32_t mMask;
    uint32_t mNotificationCount;
  };

  typedef boost::intrusive_ptr<Event> HyperspaceEventPtr;


  /**
   * EventNamed class.  Encapsulates named events (e.g. ATTR_SET,
   * ATTR_DEL, CHILD_NODE_ADDED, CHILD_NODE_REMOVED)
   */
  class EventNamed : public Event {
  public:
    EventNamed(uint64_t id, uint32_t mask, std::string name) : Event(id, mask), mName(name) { return; }
    virtual uint32_t EncodedLength() { return 12 + hypertable::Serialization::EncodedLengthString(mName); }
    virtual void Encode(hypertable::CommBuf *cbuf) { 
      cbuf->AppendLong(mId);
      cbuf->AppendInt(mMask);
      cbuf->AppendString(mName);
    }
  private:
    std::string mName;
  };


  /**
   * EventLockAcquired class.  Used to notify handles when a lock is acquired
   * on the node it points to.
   */
  class EventLockAcquired : public Event {
  public:
    EventLockAcquired(uint64_t id, uint32_t mode) : Event(id, EVENT_MASK_LOCK_ACQUIRED), mMode(mode) { return; }
    virtual uint32_t EncodedLength() { return 16; }
    virtual void Encode(hypertable::CommBuf *cbuf) { 
      cbuf->AppendLong(mId);
      cbuf->AppendInt(mMask);
      cbuf->AppendInt(mMode);
    }
  private:
    uint32_t mMode;
  };

  /**
   * EventLockReleased class.  Used to notify handles when a lock is released
   * on the node it points to.
   */
  class EventLockReleased : public Event {
  public:
    EventLockReleased(uint64_t id) : Event(id, EVENT_MASK_LOCK_RELEASED) { return; }
    virtual uint32_t EncodedLength() { return 12; }
    virtual void Encode(hypertable::CommBuf *cbuf) { 
      cbuf->AppendLong(mId);
      cbuf->AppendInt(mMask);
    }
  };

  /**
   * EventLockGranted class.  Used to notify handles that a prior lock request
   * been granted.
   */
  class EventLockGranted : public Event {
  public:
    EventLockGranted(uint64_t id, uint32_t mode, uint64_t generation) : Event(id, EVENT_MASK_LOCK_GRANTED), mMode(mode), mGeneration(generation) { return; }
    virtual uint32_t EncodedLength() { return 24; }
    virtual void Encode(hypertable::CommBuf *cbuf) { 
      cbuf->AppendLong(mId);
      cbuf->AppendInt(mMask);
      cbuf->AppendInt(mMode);
      cbuf->AppendLong(mGeneration);
    }
  private:
    uint32_t mMode;
    uint64_t mGeneration;
  };
}

#endif // HYPERSPACE_EVENT_H
