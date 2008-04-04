/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#include "ScannerTimestampController.h"

using namespace Hypertable;


void ScannerTimestampController::add_update_timestamp(Timestamp &ts) {
  boost::mutex::scoped_lock lock(m_mutex);
  m_update_timestamps.push_back(ts);
}


void ScannerTimestampController::remove_update_timestamp(Timestamp &ts) {
  boost::mutex::scoped_lock lock(m_mutex);
  UpdateTimestampContainerT::nth_index<1>::type &sorted_index = m_update_timestamps.get<1>();
  sorted_index.erase(ts);
}


bool ScannerTimestampController::get_oldest_update_timestamp(Timestamp *timestampp) {
  boost::mutex::scoped_lock lock(m_mutex);
  UpdateTimestampContainerT::const_iterator iter = m_update_timestamps.begin();
  if (iter == m_update_timestamps.end()) {
    timestampp->clear();
    return false;
  }
  *timestampp = *iter;
  return true;
}

