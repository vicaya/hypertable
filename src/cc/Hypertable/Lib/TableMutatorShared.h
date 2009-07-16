/** -*- C++ -*-
 * Copyright (C) 2009  Luke Lu (llu@hypertable.org)
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

#ifndef HYPERTABLE_TABLEMUTATOR_SHARED_H
#define HYPERTABLE_TABLEMUTATOR_SHARED_H

#include "Common/Time.h"

#include "TableMutator.h"

namespace Hypertable {

class TableMutatorIntervalHandler;

/**
 * A TableMutator that can be shared from multiple threads and incidentally
 * has an option to do periodic flushes. For best throughput use the vanilla
 * TableMutator
 */
class TableMutatorShared : public TableMutator {
  typedef TableMutator Parent;

public:
  /**
   * @param props smart pointer to the Comm layer
   * @param comm pointer to the Comm layer
   * @param table pointer to the table object
   * @param range_locator smart pointer to the range locator
   * @param app_queue pointer to the application queue
   * @param timeout_ms maximum time in milliseconds to allow methods
   *        to execute before throwing an exception
   * @param flush_interval_ms period in milliseconds to flush
   * @param flags rangeserver client update command flags
   */
  TableMutatorShared(PropertiesPtr &props, Comm *comm, Table *table,
      RangeLocatorPtr &range_locator, ApplicationQueuePtr &app_queue,
      uint32_t timeout_ms, uint32_t flush_interval_ms, uint32_t flags = 0);

  virtual ~TableMutatorShared();

  /**
   * @see TableMutator::set
   */
  virtual void set(const KeySpec &key, const void *value, uint32_t value_len) {
    ScopedRecLock lock(m_mutex);
    Parent::set(key, value, value_len);
  }

  /**
   * @see TableMutator::set_delete
   */
  virtual void set_delete(const KeySpec &key) {
    ScopedRecLock lock(m_mutex);
    Parent::set_delete(key);
  }

  /**
   * @see TableMutator::set_cells
   */
  virtual void set_cells(const Cells &cells) {
    ScopedRecLock lock(m_mutex);
    Parent::set_cells(cells);
  }

  /**
   * @see TableMutator::flush
   */
  virtual void flush() {
    ScopedRecLock lock(m_mutex);
    Parent::flush();
    m_last_flush_ts.reset();
  }

  /**
   * @see TableMutator::retry
   */
  virtual bool retry(uint32_t timeout_ms = 0) {
    ScopedRecLock lock(m_mutex);
    return Parent::retry();
  }

  /**
   * @see TableMutator:memory_used
   */
  virtual uint64_t memory_used() {
    ScopedRecLock lock(m_mutex);
    return Parent::memory_used();
  }

  /**
   * @see TableMutator::get_resend_count
   */
  virtual uint64_t get_resend_count() {
    ScopedRecLock lock(m_mutex);
    return Parent::get_resend_count();
  }

  /**
   * @see TableMutator::get_failed
   */
  virtual void get_failed(FailedMutations &failed_mutations) {
    ScopedRecLock lock(m_mutex);
    return Parent::get_failed(failed_mutations);
  }

  /**
   * @see TableMutator::need_retry
   */
  virtual bool need_retry() {
    ScopedRecLock lock(m_mutex);
    return Parent::need_retry();
  }

  uint32_t flush_interval() { return m_flush_interval; }

  /**
   * Flush if necessary considering the flush interval
   */
  void interval_flush();

protected:
  virtual void auto_flush(Timer &);

private:
  RecMutex      m_mutex;
  uint32_t      m_flush_interval;
  HiResTime     m_last_flush_ts;
  intrusive_ptr<TableMutatorIntervalHandler> m_tick_handler;
};

} // namespace Hypertable

#endif /* HYPERTABLE_TABLEMUTATOR_SHARED_H */
