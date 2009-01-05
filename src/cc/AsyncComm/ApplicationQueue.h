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

#ifndef HYPERTABLE_APPLICATIONQUEUE_H
#define HYPERTABLE_APPLICATIONQUEUE_H

#include <cassert>
#include <list>

#include <boost/thread/condition.hpp>

#include "Common/Thread.h"
#include "Common/Mutex.h"
#include "Common/Logger.h"
#include "Common/HashMap.h"
#include "Common/ReferenceCount.h"
#include "Common/StringExt.h"

#include "ApplicationHandler.h"

namespace Hypertable {

  /**
   * Provides application work queue and worker threads.  It maintains a queue
   * of requests and a pool of threads that pull requests off the queue and
   * carry them out.
   */
  class ApplicationQueue : public ReferenceCount {

    class UsageRec {
    public:
      UsageRec() : thread_group(0), running(false), outstanding(1) { return; }
      uint64_t thread_group;
      bool     running;
      int      outstanding;
    };

    typedef hash_map<uint64_t, UsageRec *> UsageRecMap;

    class WorkRec {
    public:
      WorkRec(ApplicationHandler *ah) : handler(ah), usage(0) { return; }
      ~WorkRec() { delete handler; }
      ApplicationHandler   *handler;
      UsageRec             *usage;
    };

    typedef std::list<WorkRec *> WorkQueue;

    class ApplicationQueueState {
    public:
      ApplicationQueueState() : shutdown(false) { return; }

      WorkQueue           queue;
      WorkQueue           urgent_queue;
      UsageRecMap         usage_map;
      Mutex               queue_mutex;
      Mutex               usage_mutex;
      boost::condition    cond;
      bool                shutdown;
    };

    class Worker {

    public:
      Worker(ApplicationQueueState &qstate) : m_state(qstate) { return; }

      void operator()() {
        WorkRec *rec = 0;
        WorkQueue::iterator iter;

        while (true) {

          {  // !!! maybe ditch this block specifier
            ScopedLock lock(m_state.queue_mutex);

            while (m_state.queue.empty() && m_state.urgent_queue.empty()) {
              if (m_state.shutdown)
                return;
              m_state.cond.wait(lock);
            }

            {
              ScopedLock ulock(m_state.usage_mutex);

              rec = 0;

              for (iter = m_state.urgent_queue.begin();
                   iter != m_state.urgent_queue.end(); ++iter) {
                rec = (*iter);
                if (rec->usage == 0 || !rec->usage->running) {
                  if (rec->usage)
                    rec->usage->running = true;
                  m_state.urgent_queue.erase(iter);
                  break;
                }
                rec = 0;
              }

              if (rec == 0) {
                for (iter = m_state.queue.begin(); iter != m_state.queue.end();
                     ++iter) {
                  rec = (*iter);
                  if (rec->usage == 0 || !rec->usage->running) {
                    if (rec->usage)
                      rec->usage->running = true;
                    m_state.queue.erase(iter);
                    break;
                  }
                  rec = 0;
                }
              }
            }
            if (rec == 0) {
              if (m_state.shutdown)
                return;
              m_state.cond.wait(lock);
              if (m_state.shutdown)
                return;
            }
          }

          if (rec) {
            rec->handler->run();
            if (rec->usage) {
              ScopedLock ulock(m_state.usage_mutex);
              rec->usage->running = false;
              rec->usage->outstanding--;
              if (rec->usage->outstanding == 0) {
                m_state.usage_map.erase(rec->usage->thread_group);
                delete rec->usage;
              }
            }
            delete rec;
          }
        }

        HT_INFO("thread exit");
      }

    private:
      ApplicationQueueState &m_state;
    };

    Mutex                  m_mutex;
    ApplicationQueueState  m_state;
    ThreadGroup            m_threads;
    bool joined;

  public:

    /**
     * Constructor to set up the application queue.  It creates a number
     * of worker threads specified by the worker_count argument.
     *
     * @param worker_count number of worker threads to create
     */
    ApplicationQueue(int worker_count) : joined(false) {
      Worker Worker(m_state);
      assert (worker_count > 0);
      for (int i=0; i<worker_count; ++i)
        m_threads.create_thread(Worker);
      //threads
    }

    ~ApplicationQueue() {
      if (!joined) {
        shutdown();
        join();
      }
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
      ScopedLock lock(m_mutex);
      if (!joined) {
        m_threads.join_all();
        joined = true;
      }
    }

    /**
     * Adds a request (application handler) to the request queue.  The request
     * queue is designed to support the serialization of related requests.
     * Requests are related by the thread group ID value in the
     * ApplicationHandler.  This thread group ID is constructed in the Event
     * object
     */
    void add(ApplicationHandler *app_handler) {
      UsageRecMap::iterator uiter;
      uint64_t thread_group = app_handler->get_thread_group();
      WorkRec *rec = new WorkRec(app_handler);
      rec->usage = 0;

      HT_ASSERT(app_handler);

      if (thread_group != 0) {
        ScopedLock ulock(m_state.usage_mutex);
        if ((uiter = m_state.usage_map.find(thread_group))
            != m_state.usage_map.end()) {
          rec->usage = (*uiter).second;
          rec->usage->outstanding++;
        }
        else {
          rec->usage = new UsageRec();
          rec->usage->thread_group = thread_group;
          m_state.usage_map[thread_group] = rec->usage;
        }
      }

      {
        ScopedLock lock(m_state.queue_mutex);
        if (app_handler->is_urgent())
          m_state.urgent_queue.push_back(rec);
        else
          m_state.queue.push_back(rec);
        m_state.cond.notify_one();
      }
    }
  };

  typedef boost::intrusive_ptr<ApplicationQueue> ApplicationQueuePtr;

} // namespace Hypertable

#endif // HYPERTABLE_APPLICATIONQUEUE_H
