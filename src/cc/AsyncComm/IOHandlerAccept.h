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


#ifndef HYPERTABLE_IOHANDLERACCEPT_H
#define HYPERTABLE_IOHANDLERACCEPT_H

#include "HandlerMap.h"
#include "IOHandler.h"
#include "ConnectionHandlerFactory.h"

namespace Hypertable {

  /**
   *
   */
  class IOHandlerAccept : public IOHandler {

  public:

    IOHandlerAccept(int sd, sockaddr_in &addr, DispatchHandlerPtr &dhp,
                    HandlerMapPtr &hmap, ConnectionHandlerFactoryPtr &chfp)
      : IOHandler(sd, addr, dhp), m_handler_map_ptr(hmap),
        m_handler_factory_ptr(chfp) {
      return;
    }

    virtual ~IOHandlerAccept() {
      return;
    }

    // define default poll() interface for everyone since it is chosen at runtime
    virtual bool handle_event(struct pollfd *event, clock_t arrival_clocks);

#if defined(__APPLE__)
    virtual bool handle_event(struct kevent *event, clock_t arrival_clocks);
#elif defined(__linux__)
    virtual bool handle_event(struct epoll_event *event,
                              clock_t arrival_clocks);
#elif defined(__sun__)
    virtual bool handle_event(port_event_t *event, clock_t arrival_clocks);
#else
    ImplementMe;
#endif

    bool handle_incoming_connection();

  private:
    HandlerMapPtr m_handler_map_ptr;
    ConnectionHandlerFactoryPtr m_handler_factory_ptr;
  };

  typedef intrusive_ptr<IOHandlerAccept> IOHandlerAcceptPtr;

}


#endif // HYPERTABLE_IOHANDLERACCEPT_H

