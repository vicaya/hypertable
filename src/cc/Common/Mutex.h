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

namespace Hypertable {

typedef boost::mutex::scoped_lock ScopedLock;

class Mutex : public boost::mutex {
public:
#if BOOST_VERSION < 103500
  typedef boost::detail::thread::lock_ops<boost::mutex> Ops;

  void lock() { Ops::lock(*this); }

  void unlock() { Ops::unlock(*this); }
#endif
};

} // namespace Hypertable

#endif // HYPERTABLE_MUTEX_H
