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

#ifndef HYPERSPACE_EVENT_H
#define HYPERSPACE_EVENT_H

#include "Common/Compat.h"

extern "C" {
#include <poll.h>
}

#include <iostream>
#include <string>

#include <boost/thread/condition.hpp>

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/Serialization.h"
#include "Common/System.h"

#include "AsyncComm/CommBuf.h"

#include "HandleCallback.h"
#include "BerkeleyDbFilesystem.h"

#define HT_BDBTXN_EVT_BEGIN(parent_txn) \
  do { \
    DbTxn *txn = ms_bdb_fs->start_transaction(parent_txn); \
    try

#define HT_BDBTXN_EVT_END_CB(_cb_) \
    catch (Exception &e) { \
      if (e.code() != Error::HYPERSPACE_BERKELEYDB_DEADLOCK) { \
        if (e.code() == Error::HYPERSPACE_BERKELEYDB_ERROR) \
          HT_ERROR_OUT << e << HT_END; \
        else \
          HT_WARNF("%s - %s", Error::get_text(e.code()), e.what()); \
        txn->abort(); \
        _cb_->error(e.code(), e.what()); \
        return; \
      } \
      HT_WARN_OUT << "Berkeley DB deadlock encountered in txn "<< txn << HT_END; \
      txn->abort(); \
      poll(0, 0, (System::rand32() % 3000) + 1); \
      continue; \
    } \
    break; \
  } while (true)

#define HT_BDBTXN_EVT_END(...) \
    catch (Exception &e) { \
      if (e.code() != Error::HYPERSPACE_BERKELEYDB_DEADLOCK) { \
        if (e.code() == Error::HYPERSPACE_BERKELEYDB_ERROR) \
          HT_ERROR_OUT << e << HT_END; \
        else \
          HT_WARNF("%s - %s", Error::get_text(e.code()), e.what()); \
        txn->abort(); \
        return __VA_ARGS__; \
      } \
      HT_WARN_OUT << "Berkeley DB deadlock encountered in txn "<< txn << HT_END; \
      txn->abort(); \
      poll(0, 0, (System::rand32() % 3000) + 1); \
      continue; \
    } \
    break; \
  } while (true)

namespace Hyperspace {
  using namespace Hypertable;
  enum {
    EVENT_TYPE_NAMED = 1,
    EVENT_TYPE_LOCK_ACQUIRED,
    EVENT_TYPE_LOCK_RELEASED,
    EVENT_TYPE_LOCK_GRANTED
  };

  class Event : public ReferenceCount {
  public:
    Event(uint64_t id, uint32_t mask) : m_id(id), m_mask(mask), m_notification_count(0) {
    }
    virtual ~Event() { return; }

    uint64_t get_id() { return m_id; }

    uint32_t get_mask() { return m_mask; }

    void increment_notification_count() {
      ScopedLock lock(m_mutex);
      m_notification_count++;
    }

    void decrement_notification_count() {
      ScopedLock lock(m_mutex);
      m_notification_count--;
      if (m_notification_count == 0) {
        // all notifications received, so delete event from BDB
        HT_BDBTXN_EVT_BEGIN() {
          ms_bdb_fs->delete_event(txn, m_id);
          txn->commit(0);
        }
        HT_BDBTXN_EVT_END();
        m_cond.notify_all();
      }
    }

    void wait_for_notifications() {
      ScopedLock lock(m_mutex);
      if (m_notification_count != 0)
        m_cond.wait(lock);
    }

    virtual uint32_t encoded_length() = 0;
    virtual void encode(Hypertable::CommBuf *cbuf) = 0;

    static void set_bdb_fs(BerkeleyDbFilesystem *bdb_fs) {
      ms_bdb_fs = bdb_fs;
    }

  protected:
    static            BerkeleyDbFilesystem *ms_bdb_fs;
    Mutex             m_mutex;
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
    EventNamed(uint64_t id, uint32_t mask, const std::string &name)
        : Event(id, mask), m_name(name) { return; }

    virtual uint32_t encoded_length() {
      return 12 + Hypertable::Serialization::encoded_length_vstr(m_name);
    }

    virtual void encode(Hypertable::CommBuf *cbuf) {
      cbuf->append_i64(m_id);
      cbuf->append_i32(m_mask);
      cbuf->append_vstr(m_name);
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
    EventLockAcquired(uint64_t id, uint32_t mode)
      : Event(id, EVENT_MASK_LOCK_ACQUIRED), m_mode(mode) { return; }

    virtual uint32_t encoded_length() { return 16; }
    virtual void encode(Hypertable::CommBuf *cbuf) {
      cbuf->append_i64(m_id);
      cbuf->append_i32(m_mask);
      cbuf->append_i32(m_mode);
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
    EventLockReleased(uint64_t id) : Event(id, EVENT_MASK_LOCK_RELEASED) { return; }
    virtual uint32_t encoded_length() { return 12; }
    virtual void encode(Hypertable::CommBuf *cbuf) {
      cbuf->append_i64(m_id);
      cbuf->append_i32(m_mask);
    }
  };

  /**
   * EventLockGranted class.  Used to notify handles that a prior lock request
   * been granted.
   */
  class EventLockGranted : public Event {
  public:
    EventLockGranted(uint64_t id, uint32_t mode, uint64_t generation)
      : Event(id, EVENT_MASK_LOCK_GRANTED), m_mode(mode), m_generation(generation)
      { }

    virtual uint32_t encoded_length() { return 24; }
    virtual void encode(Hypertable::CommBuf *cbuf) {
      cbuf->append_i64(m_id);
      cbuf->append_i32(m_mask);
      cbuf->append_i32(m_mode);
      cbuf->append_i64(m_generation);
    }
  private:
    uint32_t m_mode;
    uint64_t m_generation;
  };
} // namespace Hyperspace

#endif // HYPERSPACE_EVENT_H
