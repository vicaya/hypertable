/**
 * Copyright (C) 2007 Luke Lu (Zvents, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef HYPERTABLE_MUTEX_H
#define HYPERTABLE_MUTEX_H

#include <boost/version.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>

#include "Common/Logger.h"

namespace Hypertable {

/** boost::mutex::scoped_lock use lock_ops<mutex>::lock/unlock in pre 1.35
 * which makes it less convenient than the following which can be used
 * to guard any object (besides a typical mutex) with lock/unlock methods
 */
template <class MutexT>
class Locker : boost::noncopyable {
public:
  explicit Locker(MutexT &mutex, bool init_lock = true)
    : m_mutex(mutex), m_locked(false) {
    if (init_lock) lock();
  }
  ~Locker() {
    if (m_locked) unlock();
  }

private:
  MutexT &m_mutex;
  bool m_locked;

  void lock() {
    HT_ASSERT(!m_locked);
    m_mutex.lock();
    m_locked = true;
  }

  void unlock() {
    HT_ASSERT(m_locked);
    m_mutex.unlock();
    m_locked = false;
  }
};

typedef boost::mutex::scoped_lock ScopedLock;

class Mutex : public boost::mutex {
public:
#if BOOST_VERSION < 103500
  typedef boost::detail::thread::lock_ops<boost::mutex> Ops;

  void lock() { Ops::lock(*this); }

  void unlock() { Ops::unlock(*this); }
#endif
};

class RecMutex : public boost::recursive_mutex {
public:
#if BOOST_VERSION < 103500
  typedef boost::detail::thread::lock_ops<boost::recursive_mutex> Ops;

  void lock() { Ops::lock(*this); }

  void unlock() { Ops::unlock(*this); }
#endif
};

typedef boost::recursive_mutex::scoped_lock ScopedRecLock;

} // namespace Hypertable

#endif // HYPERTABLE_MUTEX_H
