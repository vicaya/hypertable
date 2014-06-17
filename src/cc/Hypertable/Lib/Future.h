/**
 * Copyright (C) 2011 Hypertable, Inc.
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

#ifndef HYPERTABLE_FUTURE_H
#define HYPERTABLE_FUTURE_H

#include <boost/thread/condition.hpp>
#include <list>
#include <map>

#include "ResultCallback.h"
#include "Result.h"

namespace Hypertable {

  using namespace std;
  class Future : public ResultCallback {
  public:

    /**
     * Future objects are used to access results from asynchronous scanners/mutators
     * @param capacity number of Result objects to enqueue
     */
    Future(size_t capacity) : m_queue_capacity(capacity), m_cancelled(false) { return; }
    ~Future() { cancel(); }

    /**
     * This call blocks till there is a result available unless async ops have completed
     * @param result will contain a reference to the result object
     * @return true if asynchronous operations have completed
     */
    bool get(ResultPtr &result);

    /**
     * This call blocks for the lesser of timeout / time till there is a result available
     * @param result will contain a reference to the result object
     * @param timeout_ms max milliseconds to block for
     * @param timed_out set to true if the call times out
     * @return true if asynchronous operations have completed
     */
    bool get(ResultPtr &result, uint32_t timeout_ms, bool &timed_out);

    /**
     * Cancels outstanding scanners/mutators
     */
    void cancel();

    void register_scanner(TableScannerAsync *scanner);

    void deregister_scanner(TableScannerAsync *scanner);

  private:
    friend class TableScannerAsync;
    friend class TableMutator;
    typedef list<ResultPtr> ResultQueue;

    void scan_ok(TableScannerAsync *scanner, ScanCellsPtr &cells);
    void scan_error(TableScannerAsync *scanner, int error, const String &error_msg,
                    bool eos);
    void update_ok(TableMutator *mutator, FailedMutations &failed_mutations);
    void update_error(TableMutator *mutator, int error, const String &error_msg);

    bool is_full() { return (m_queue_capacity <= m_queue.size()); }

    bool is_empty() { return m_queue.empty(); }
    bool is_cancelled() const {
      return m_cancelled;
    }

    void enqueue(ResultPtr &result);

    ResultQueue m_queue;
    size_t m_queue_capacity;
    boost::condition m_cond;
    Mutex m_mutex;
    bool m_cancelled;
    typedef map<uint64_t, TableScannerAsync *> ScannerMap;
    ScannerMap m_scanner_map;
  };
  typedef intrusive_ptr<Future> FuturePtr;
}

#endif // HYPERTABLE_FUTURE_H
