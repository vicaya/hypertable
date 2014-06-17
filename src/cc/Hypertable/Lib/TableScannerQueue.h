/** -*- c++ -*-
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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_TABLESCANNERQUEUE_H
#define HYPERTABLE_TABLESCANNERQUEUE_H

#include <list>
#include <boost/thread/condition.hpp>

#include "Common/Thread.h"
#include "Common/Mutex.h"
#include "AsyncComm/ApplicationQueue.h"

#include "ScanCells.h"

namespace Hypertable {

  /**
   * Provides application work queue and worker threads.  It maintains a queue
   * of requests and a pool of threads that pull requests off the queue and
   * carry them out.
   */
  class TableScannerQueue : public ApplicationQueue {

  public:

    /**
     *
     * @param results_capacity max number of ScanCells that can be enqueued
     */
    TableScannerQueue() : m_error(Error::OK){ }

    ~TableScannerQueue () { }

    /**
     */
    virtual void add(ApplicationHandler *app_handler) {
      ScopedLock lock(m_mutex);
      m_work_queue.push_back(app_handler);
      m_cond.notify_one();
    }

    void next_result(ScanCellsPtr &cells, int *error, String &error_msg) {
      ApplicationHandler *app_handler;
      cells = 0;
      *error = Error::OK;
      while(true) {
        {
          ScopedLock lock(m_mutex);
          if (m_error != Error::OK) {
            *error = m_error;
            error_msg = m_error_msg;
            break;
          }
          else if (!m_cells_queue.empty()) {
            cells = m_cells_queue.front();
            m_cells_queue.pop_front();
            break;
          }
          while (m_work_queue.empty()) {
            m_cond.wait(lock);
          }
          app_handler = m_work_queue.front();
          HT_ASSERT(app_handler);
          m_work_queue.pop_front();
        }
        app_handler->run();
        delete app_handler;
      }
      HT_ASSERT(cells != 0 || *error != Error::OK);
    }


    void add_cells(ScanCellsPtr &cells) {
      m_cells_queue.push_back(cells);
    }

    void set_error(int error, const String &error_msg) {
      m_error = error;
      m_error_msg = error_msg;
    }

    /**
     * Override unused inherited methods
     */
    virtual void stop() { }
    virtual void start() { }
    virtual void join() { }
    virtual void shutdown() { }

  private:

    typedef std::list<ApplicationHandler *> WorkQueue;
    typedef std::list<ScanCellsPtr> CellsQueue;
    Mutex                  m_mutex;
    boost::condition       m_cond;
    WorkQueue              m_work_queue;
    CellsQueue             m_cells_queue;
    int                    m_error;
    String                 m_error_msg;
  };

  typedef boost::intrusive_ptr<TableScannerQueue> TableScannerQueuePtr;

} // namespace Hypertable

#endif // HYPERTABLE_TABLESCANNERQUEUE_H
