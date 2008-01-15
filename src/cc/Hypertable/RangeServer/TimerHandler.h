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

#ifndef HYPERSPACE_TIMERHANDLER_H
#define HYPERSPACE_TIMERHANDLER_H

#include <boost/shared_ptr.hpp>

#include "AsyncComm/Comm.h"
#include "AsyncComm/DispatchHandler.h"

#include "RangeServer.h"

using namespace Hypertable;

namespace Hyperspace {

  /**
   */
  class TimerHandler : public DispatchHandler {
  public:
    TimerHandler(Comm *comm, RangeServer *range_server);
    virtual void handle(Hypertable::EventPtr &event_ptr);

  private:
    Comm         *m_comm;
    RangeServer  *m_range_server;
  };
  typedef boost::intrusive_ptr<TimerHandler> TimerHandlerPtr;
}

#endif // HYPERSPACE_TIMERHANDLER_H

