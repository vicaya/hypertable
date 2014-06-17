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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"

#include "Common/Config.h"
#include "Common/System.h"
#include "Common/SystemInfo.h"

#include "HandlerMap.h"
#include "ReactorFactory.h"
#include "ReactorRunner.h"
using namespace Hypertable;

#include <cassert>

extern "C" {
#include <signal.h>
}

std::vector<ReactorPtr> ReactorFactory::ms_reactors;
boost::thread_group ReactorFactory::ms_threads;
Mutex        ReactorFactory::ms_mutex;
atomic_t     ReactorFactory::ms_next_reactor = ATOMIC_INIT(0);
bool         ReactorFactory::ms_epollet = true;
bool         ReactorFactory::use_poll = false;
bool         ReactorFactory::proxy_master = false;

/**
 */
void ReactorFactory::initialize(uint16_t reactor_count) {
  ScopedLock lock(ms_mutex);
  if (!ms_reactors.empty())
    return;
  ReactorPtr reactor_ptr;
  ReactorRunner rrunner;
  ReactorRunner::handler_map = new HandlerMap();
  signal(SIGPIPE, SIG_IGN);
  assert(reactor_count > 0);

#if defined(__linux__)
  if (System::os_info().version_major < 2 ||
      System::os_info().version_minor < 6 ||
      (System::os_info().version_major == 2 &&
       System::os_info().version_minor == 6 &&
       System::os_info().version_micro < 17))
    ms_epollet = false;
  if (System::os_info().version_major < 2 ||
      System::os_info().version_minor < 5)
    use_poll = true;
#endif

  if (Config::properties->get_bool("Comm.UsePoll") == true)
    use_poll = true;

  for (uint16_t i=0; i<reactor_count; i++) {
    reactor_ptr = new Reactor();
    ms_reactors.push_back(reactor_ptr);
    rrunner.set_reactor(reactor_ptr);
    ms_threads.create_thread(rrunner);
  }
}

void ReactorFactory::destroy() {
  ReactorRunner::shutdown = true;
  for (size_t i=0; i<ms_reactors.size(); i++)
    ms_reactors[i]->poll_loop_interrupt();
  ms_threads.join_all();
  ms_reactors.clear();
  ReactorRunner::handler_map = 0;
}
