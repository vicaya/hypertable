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

  class Event : public Hypertable::ReferenceCount {
  public:
    Event(uint32_t mask) : m_mask(mask), m_notification_count(0) { 
      boost::mutex::scoped_lock lock(ms_next_event_id_mutex);
      m_id = ms_next_event_id++;
      return; 
    }
    virtual ~Event() { return; }

    uint64_t get_id() { return m_id; }

    uint32_t get_mask() { return m_mask; }

    void increment_notification_count() {
      boost::mutex::scoped_lock lock(m_mutex);
      m_notification_count++;
    }
    void decrement_notification_count() {
      boost::mutex::scoped_lock lock(m_mutex);
      m_notification_count--;
      if (m_notification_count == 0)
	m_cond.notify_all();
    }
    void wait_for_notifications() {
      boost::mutex::scoped_lock lock(m_mutex);
      if (m_notification_count != 0)
	m_cond.wait(lock);
    }

    virtual uint32_t encoded_length() = 0;
    virtual void encode(Hypertable::CommBuf *cbuf) = 0;

  protected:

    static boost::mutex  ms_next_event_id_mutex;
    static uint64_t      ms_next_event_id;

    boost::mutex      m_mutex;
    boost::condition  m_cond;
    uint64_t m_id;
    uint32_t m_mask;
    uint32_t m_notification_count;
  };

  typedef boost::intrusive_ptr<Event> HyperspaceEventPtr;


  /**
   * EventNamed class.  Encapsulates named events (e.g. ATTR_SET,
   * ATTR_DEL, CHILD_NODE_ADDED, CHILD_NODE_REMOVED)
   */
  class EventNamed : public Event {
  public:
    EventNamed(uint32_t mask, std::string name) : Event(mask), m_name(name) { return; }
    virtual uint32_t encoded_length() { return 12 + Hypertable::Serialization::encoded_length_string(m_name); }
    virtual void encode(Hypertable::CommBuf *cbuf) { 
      cbuf->append_long(m_id);
      cbuf->append_int(m_mask);
      cbuf->append_string(m_name);
    }
  private:
    std::string m_name;
  };


  /**
   * EventLockAcquired class.  Used to notify handles when a lock is acquired
   * on the node it points to.
   */
  class EventLockAcquired : public Event {
  public:
    EventLockAcquired(uint32_t mode) : Event(EVENT_MASK_LOCK_ACQUIRED), m_mode(mode) { return; }
    virtual uint32_t encoded_length() { return 16; }
    virtual void encode(Hypertable::CommBuf *cbuf) { 
      cbuf->append_long(m_id);
      cbuf->append_int(m_mask);
      cbuf->append_int(m_mode);
    }
  private:
    uint32_t m_mode;
  };

  /**
   * EventLockReleased class.  Used to notify handles when a lock is released
   * on the node it points to.
   */
  class EventLockReleased : public Event {
  public:
    EventLockReleased() : Event(EVENT_MASK_LOCK_RELEASED) { return; }
    virtual uint32_t encoded_length() { return 12; }
    virtual void encode(Hypertable::CommBuf *cbuf) { 
      cbuf->append_long(m_id);
      cbuf->append_int(m_mask);
    }
  };

  /**
   * EventLockGranted class.  Used to notify handles that a prior lock request
   * been granted.
   */
  class EventLockGranted : public Event {
  public:
    EventLockGranted(uint32_t mode, uint64_t generation) : Event(EVENT_MASK_LOCK_GRANTED), m_mode(mode), m_generation(generation) { return; }
    virtual uint32_t encoded_length() { return 24; }
    virtual void encode(Hypertable::CommBuf *cbuf) { 
      cbuf->append_long(m_id);
      cbuf->append_int(m_mask);
      cbuf->append_int(m_mode);
      cbuf->append_long(m_generation);
    }
  private:
    uint32_t m_mode;
    uint64_t m_generation;
  };
}

#endif // HYPERSPACE_EVENT_H
