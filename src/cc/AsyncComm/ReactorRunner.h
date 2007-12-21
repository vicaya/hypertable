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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_REACTORRUNNER_H
#define HYPERTABLE_REACTORRUNNER_H

#include "HandlerMap.h"
#include "Reactor.h"

namespace Hypertable {

  class IOHandler;

  /**
   *
   */
  class ReactorRunner {
  public:
    void operator()();
    void set_reactor(ReactorPtr &reactor_ptr) { m_reactor_ptr = reactor_ptr; }
    static bool ms_shutdown;
    static HandlerMapPtr ms_handler_map_ptr;
  private:
    void cleanup_and_remove_handlers(set<IOHandler *> &handlers);
    ReactorPtr m_reactor_ptr;
  };

}

#endif // HYPERTABLE_REACTORRUNNER_H
