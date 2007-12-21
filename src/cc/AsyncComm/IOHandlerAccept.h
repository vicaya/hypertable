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


#ifndef HYPERTABLE_IOHANDLERACCEPT_H
#define HYPERTABLE_IOHANDLERACCEPT_H

#include "HandlerMap.h"
#include "IOHandler.h"
#include "ConnectionHandlerFactory.h"

extern "C" {
#include <stdint.h>
}

namespace Hypertable {

  /**
   *
   */
  class IOHandlerAccept : public IOHandler {

  public:

    IOHandlerAccept(int sd, struct sockaddr_in &addr, DispatchHandlerPtr &dhp, HandlerMapPtr &handler_map_ptr, ConnectionHandlerFactoryPtr &chfPtr) : IOHandler(sd, addr, dhp), m_handler_map_ptr(handler_map_ptr), m_handler_factory_ptr(chfPtr) {
      return;
    }

    virtual ~IOHandlerAccept() {
      return;
    }

#if defined(__APPLE__)
    virtual bool handle_event(struct kevent *event);
#elif defined(__linux__)
    virtual bool handle_event(struct epoll_event *event);
#else
    ImplementMe;
#endif

    bool handle_incoming_connection();

  private:
    HandlerMapPtr m_handler_map_ptr;
    ConnectionHandlerFactoryPtr m_handler_factory_ptr;
  };

  typedef boost::intrusive_ptr<IOHandlerAccept> IOHandlerAcceptPtr;

}


#endif // HYPERTABLE_IOHANDLERACCEPT_H

