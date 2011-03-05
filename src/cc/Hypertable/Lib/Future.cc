/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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
#include <boost/thread/xtime.hpp>

#include "Future.h"
#include "TableScannerAsync.h"

using namespace Hypertable;

bool Future::get(ResultPtr &result) {
  ScopedLock lock(m_mutex);

  // wait till we have results to serve
  while(is_empty() && !is_done() && !is_cancelled()) {
    m_cond.wait(lock);
  }

  if (is_cancelled())
    return false;
  if (is_empty() && is_done())
    return false;
  result = m_queue.front();
  // wake a thread blocked on queue space
  m_queue.pop_front();
  m_cond.notify_one();
  return true;
}

bool Future::get(ResultPtr &result, uint32_t timeout_ms, bool &timed_out) {
  ScopedLock lock(m_mutex);

  timed_out = false;

  boost::xtime wait_time;
  boost::xtime_get(&wait_time, boost::TIME_UTC);
  xtime_add_millis(wait_time, timeout_ms);

  // wait till we have results to serve
  while(is_empty() && !is_done() && !is_cancelled()) {
    timed_out = m_cond.timed_wait(lock, wait_time);
    if (timed_out)
      return is_done();
  }
  if (is_cancelled())
    return false;
  if (is_empty() && is_done())
    return false;
  result = m_queue.front();
  // wake a thread blocked on queue space
  m_queue.pop_front();
  m_cond.notify_one();
  return true;
}

void Future::scan_ok(TableScannerAsync *scanner, ScanCellsPtr &cells) {
  ResultPtr result = new Result(scanner, cells);
  enqueue(result);
}

void Future::enqueue(ResultPtr &result) {
  ScopedLock lock(m_mutex);
  while (is_full() && !is_cancelled()) {
    m_cond.wait(lock);
  }
  if (!is_cancelled()) {
    m_queue.push_back(result);
  }
  m_cond.notify_one();
}

void Future::scan_error(TableScannerAsync *scanner, int error, const String &error_msg,
                        bool eos) {
  ResultPtr result = new Result(scanner, error, error_msg);
  enqueue(result);
}

void Future::update_ok(TableMutator *mutator, FailedMutations &failed_mutations) {
  ResultPtr result = new Result(mutator, failed_mutations);
  enqueue(result);
}

void Future::update_error(TableMutator *mutator, int error, const String &error_msg) {
  ResultPtr result = new Result(mutator, error, error_msg);
  enqueue(result);
}

void Future::cancel() {
  ScopedLock lock(m_mutex);
  m_cancelled = true;
  ScannerMap::iterator it = m_scanner_map.begin();
  while (it != m_scanner_map.end()) {
    it->second->cancel();
    it++;
  }
  m_cond.notify_all();

}

void Future::register_scanner(TableScannerAsync *scanner) {
  ScopedLock lock(m_mutex);
  uint64_t addr = (uint64_t) scanner;
  ScannerMap::iterator it = m_scanner_map.find(addr);
  HT_ASSERT(it == m_scanner_map.end());
  m_scanner_map[addr] = scanner;
}

void Future::deregister_scanner(TableScannerAsync *scanner) {
  ScopedLock lock(m_mutex);
  uint64_t addr = (uint64_t) scanner;
  ScannerMap::iterator it = m_scanner_map.find(addr);
  HT_ASSERT(it != m_scanner_map.end());
  m_scanner_map.erase(it);
}
