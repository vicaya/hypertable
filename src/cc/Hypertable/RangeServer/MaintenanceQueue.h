/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_MAINTENANCEQUEUE_H
#define HYPERTABLE_MAINTENANCEQUEUE_H

#include <cassert>

#include "MaintenanceTask.h"

namespace Hypertable {

  /**
   */
  class MaintenanceQueue : public ReferenceCount {

    struct ltMaintenanceTask {
      bool operator()(const MaintenanceTask *sm1, const MaintenanceTask *sm2) const {
	return xtime_cmp(sm1->start_time, sm2->start_time) >= 0;
      }
    };

    typedef std::priority_queue<MaintenanceTask *, std::vector<MaintenanceTask *>, ltMaintenanceTask> MaintenanceQueueT;

    class MaintenanceQueueState {
    public:
      MaintenanceQueueState() : shutdown(false) { return; }
      MaintenanceQueueT  queue;
      boost::mutex       mutex;
      boost::condition   cond;
      bool               shutdown;
    };

    class Worker {

    public:

      Worker(MaintenanceQueueState &state) : m_state(state) { return; }

      void operator()() {
	boost::xtime now, next_work;
	MaintenanceTask *task;

	while (true) {

	  {
	    boost::mutex::scoped_lock lock(m_state.mutex);

	    boost::xtime_get(&now, boost::TIME_UTC);

	    while (m_state.queue.empty() || xtime_cmp((m_state.queue.top())->start_time, now) > 0) {

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
	  }

	  try {
	    task->execute();
	  }
	  catch(Hypertable::Exception &e) {
	    HT_ERRORF("%s '%s'", e.code(), e.what());
	  }

	  delete task;
	}
      }

    private:
      MaintenanceQueueState &m_state;
    };

    MaintenanceQueueState  m_state;
    boost::thread_group    m_threads;
    bool joined;

  public:

    /**
     * Constructor to set up the application queue.  It creates a number
     * of worker threads specified by the worker_count argument.
     *
     * @param worker_count number of worker threads to create
     */
    MaintenanceQueue(int worker_count) : joined(false) {
      Worker Worker(m_state);
      assert (worker_count > 0);
      for (int i=0; i<worker_count; ++i)
	m_threads.create_thread(Worker);
      //threads
    }

    /**
     * Shuts down the application queue.  All outstanding requests are carried
     * out and then all threads exit.  #join can be called to wait for completion
     * of the shutdown.
     */
    void shutdown() {
      m_state.shutdown = true;
      m_state.cond.notify_all();
    }

    /**
     * Waits for a shutdown to complete.  This method returns when all application
     * queue threads exit.
     */
    void join() {
      if (!joined) {
	m_threads.join_all();
	joined = true;
      }
    }

    /**
     * Adds a request (application handler) to the request queue.  The request queue
     * is designed to support the serialization of related requests.  Requests are
     * related by the thread group ID value in the ApplicationHandler.  This thread
     * group ID is constructed in the Event object
     */
    void add(MaintenanceTask *task) {
      boost::mutex::scoped_lock lock(m_state.mutex);
      m_state.queue.push(task);
      m_state.cond.notify_one();
    }
  };
  typedef boost::intrusive_ptr<MaintenanceQueue> MaintenanceQueuePtr;

}


#endif // HYPERTABLE_MAINTENANCEQUEUE_H
