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


#ifndef HYPERTABLE_REACTORFACTORY_H
#define HYPERTABLE_REACTORFACTORY_H

#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

extern "C" {
#include <stdint.h>
}

#include <cassert>
#include <vector>
using namespace std;

#include "Common/atomic.h"
#include "Reactor.h"


namespace Hypertable {

  /** This class is a static class that is used to setup and manage I/O reactors.
   * Since the I/O reactor threads are a process-wide resource, the methods of
   * this class are static.
   */
  class ReactorFactory {

  public:

    /** This method creates and initializes the I/O reactor threads and must be
     * called once by an application prior to creating the Comm object.
     *
     * @param reactor_count number of reactor threads to create
     */
    static void initialize(uint16_t reactor_count);

    /** This method shuts down the reactors
     */
    static void destroy();

    /** This method returns the 'next' reactor.  It returns pointers to reactors
     * in round-robin fashion and is used by the Comm subsystem to evenly distribute
     * descriptors across all of the reactors.
     */
    static void get_reactor(ReactorPtr &reactor_ptr) {
      assert(ms_reactors.size() > 0);
      reactor_ptr = ms_reactors[atomic_inc_return(&ms_next_reactor) % ms_reactors.size()];
    }

    /** vector of reactors */
    static vector<ReactorPtr> ms_reactors;

    static boost::thread_group ms_threads;

  private:
    static boost::mutex ms_mutex;
    static atomic_t ms_next_reactor;

  };

}

#endif // HYPERTABLE_REACTORFACTORY_H

