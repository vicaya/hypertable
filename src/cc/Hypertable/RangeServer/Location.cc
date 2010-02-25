/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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
#include "Common/Compat.h"
#include "Common/Logger.h"
#include "Common/Mutex.h"

#include <boost/thread/condition.hpp>

#include "Location.h"

using namespace Hypertable;

namespace {
  Mutex mutex;
  boost::condition cond;
  String location_str;
}

void Location::set(const String &str) {
  ScopedLock lock(mutex);
  if (location_str == "")
    location_str = str;
  cond.notify_all();
}

String Location::get() {
  ScopedLock lock(mutex);
  return location_str;
}

void Location::wait_until_assigned() {
  ScopedLock lock(mutex);
  while (location_str == "") {
    HT_INFO("Waiting to be assigned a location...");
    cond.wait(lock);
  }
}
