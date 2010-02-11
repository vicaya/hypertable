/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_MAINTENANCEQUEUE_H
#define HYPERTABLE_MAINTENANCEQUEUE_H

#include <cassert>
#include <queue>
#include <set>

#include <boost/thread/condition.hpp>

#include "Common/Mutex.h"
#include "Common/Thread.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/ReferenceCount.h"

#include "MaintenanceTask.h"
#include "MaintenanceTaskMemoryPurge.h"

namespace Hypertable {

  /**
   */
  class MaintenanceQueue : public ReferenceCount {

    static int              ms_pause;
    static boost::condition ms_cond;

    struct LtMaintenanceTask {
      bool
      operator()(const MaintenanceTask *sm1, const MaintenanceTask *sm2) const {
        int cmp = xtime_cmp(sm1->start_time, sm2->start_time);
        if (cmp == 0)
          return sm1->priority < sm2->priority;
        return xtime_cmp(sm1->start_time, sm2->start_time) >= 0;
      }
    };

    typedef std::priority_queue<MaintenanceTask *,
            std::vector<MaintenanceTask *>, LtMaintenanceTask> TaskQueue;

    class MaintenanceQueueState {
    public:
      MaintenanceQueueState() : shutdown(false) { return; }
      TaskQueue          queue;
      Mutex              mutex;
      boost::condition   cond;
      boost::condition   empty_cond;
      bool               shutdown;
      std::set<Range *>  pending;
      std::set<Range *>  in_progress;
    };

    class Worker {

    public:

      Worker(MaintenanceQueueState &state) : m_state(state) { return; }

      void operator()() {
        boost::xtime now, next_work;
        MaintenanceTask *task = 0;

        while (true) {

          {
            ScopedLock lock(m_state.mutex);

            boost::xtime_get(&now, boost::TIME_UTC);

            while (m_state.queue.empty() ||
                   xtime_cmp((m_state.queue.top())->start_time, now) > 0) {

              if (m_state.shutdown)
                return;

              if (m_state.queue.empty())
                m_state.cond.wait(lock);
              else {
                next_work = (m_state.queue.top())->start_time;
                m_state.cond.timed_wait(lock, next_work);
              }
              boost::xtime_get(&now, boost::TIME_UTC);
            }

            task = m_state.queue.top();
            m_state.queue.pop();
            m_state.pending.erase(task->get_range());
            m_state.in_progress.insert(task->get_range());
          }

          try {

            // maybe pause
            {
              ScopedLock lock(m_state.mutex);
              while (ms_pause)
                ms_cond.wait(lock);
            }

            task->execute();

          }
          catch(Hypertable::Exception &e) {
            if (dynamic_cast<MaintenanceTaskMemoryPurge *>(task) == 0) {
              HT_ERROR_OUT << e << HT_END;
              if (task->retry()) {
                ScopedLock lock(m_state.mutex);
                HT_ERRORF("Maintenance Task '%s' failed, will retry in %u milliseconds ...",
                          task->description().c_str(), task->get_retry_delay());
                boost::xtime_get(&task->start_time, boost::TIME_UTC);
                task->start_time.sec += task->get_retry_delay() / 1000;
                m_state.queue.push(task);
                m_state.cond.notify_one();
                continue;
              }
              HT_ERRORF("Maintenance Task '%s' failed, dropping task ...",
                        task->description().c_str());
            }
          }

          {
            ScopedLock lock(m_state.mutex);
            m_state.in_progress.erase(task->get_range());
	    if (m_state.queue.empty() && m_state.in_progress.empty())
	      m_state.empty_cond.notify_one();
          }

          delete task;
        }
      }

    private:
      MaintenanceQueueState &m_state;
    };

    MaintenanceQueueState  m_state;
    ThreadGroup m_threads;
    int  m_workers;
    bool joined;

  public:

    /**
     * Constructor to set up the application queue.  It creates a number
     * of worker threads specified by the worker_count argument.
     *
     * @param worker_count number of worker threads to create
     */
    MaintenanceQueue(int worker_count) : m_workers(worker_count), joined(false) {
      Worker Worker(m_state);
      assert (worker_count > 0);
      for (int i=0; i<worker_count; ++i)
        m_threads.create_thread(Worker);
      //threads
    }

    /**
     * Shuts down the application queue.  All outstanding requests are carried
     * out and then all threads exit.  #join can be called to wait for
     * completion of the shutdown.
     */
    void shutdown() {
      m_state.shutdown = true;
      m_state.cond.notify_all();
    }

    /**
     * Waits for a shutdown to complete.  This method returns when all
     * application queue threads exit.
     */
    void join() {
      if (!joined) {
        m_threads.join_all();
        joined = true;
      }
    }

    /**
     * Stops queue processing
     */
    void stop() {
      ScopedLock lock(m_state.mutex);
      HT_INFO("Stopping maintenance queue");
      ms_pause++;
    }

    /**
     * Starts queue processing
     */
    void start() {
      ScopedLock lock(m_state.mutex);
      HT_ASSERT(ms_pause > 0);
      ms_pause--;
      if (ms_pause == 0) {
        ms_cond.notify_all();
        HT_INFO("Starting maintenance queue");
      }
    }

    /**
     * Clear the queue
     */
    void clear() {
      ScopedLock lock(m_state.mutex);
      while (!m_state.queue.empty())
        m_state.queue.pop();
      m_state.pending.clear();
    }

    bool is_scheduled(Range *range) {
      ScopedLock lock(m_state.mutex);
      if (m_state.pending.count(range) || m_state.in_progress.count(range))
        return true;
      return false;
    }

    /**
     * Adds a request (application handler) to the request queue.  The request
     * queue is designed to support the serialization of related requests.
     * Requests are related by the thread group ID value in the
     * ApplicationHandler.  This thread group ID is constructed in the Event
     * object
     */
    void add(MaintenanceTask *task) {
      ScopedLock lock(m_state.mutex);
      m_state.queue.push(task);
      m_state.pending.insert(task->get_range());
      m_state.cond.notify_one();
    }

    size_t workers() { return m_workers; }

    size_t pending() {
      ScopedLock lock(m_state.mutex);
      return m_state.pending.size();
    }

    void wait_for_empty() {
      ScopedLock lock(m_state.mutex);
      while (!m_state.queue.empty() || !m_state.in_progress.empty())
	m_state.empty_cond.wait(lock);
    }

  };

  typedef boost::intrusive_ptr<MaintenanceQueue> MaintenanceQueuePtr;

} // namespace Hypertable

#endif // HYPERTABLE_MAINTENANCEQUEUE_H
