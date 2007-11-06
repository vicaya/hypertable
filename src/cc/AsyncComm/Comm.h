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

#ifndef HYPERTABLE_COMMENGINE_H
#define HYPERTABLE_COMMENGINE_H

#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/xtime.hpp>

extern "C" {
#include <stdint.h>
}

#include "DispatchHandler.h"
#include "CommBuf.h"
#include "ConnectionHandlerFactory.h"
#include "HandlerMap.h"

using namespace std;

namespace hypertable {

  class Comm {

  public:

    Comm();

    ~Comm();

    int connect(struct sockaddr_in &addr, DispatchHandlerPtr &defaultHandlerPtr);

    int connect(struct sockaddr_in &addr, struct sockaddr_in &localAddr, DispatchHandlerPtr &defaultHandlerPtr);

    int listen(struct sockaddr_in &addr, ConnectionHandlerFactoryPtr &chfPtr);

    int listen(struct sockaddr_in &addr, ConnectionHandlerFactoryPtr &chfPtr, DispatchHandlerPtr &defaultHandlerPtr);

    int send_request(struct sockaddr_in &addr, time_t timeout, CommBufPtr &cbufPtr, DispatchHandler *responseHandler);

    int send_response(struct sockaddr_in &addr, CommBufPtr &cbufPtr);

    int get_local_address(struct sockaddr_in addr, struct sockaddr_in *localAddr);

    int create_datagram_receive_socket(struct sockaddr_in *addr, DispatchHandlerPtr &handlerPtr);

    int send_datagram(struct sockaddr_in &addr, struct sockaddr_in &sendAddr, CommBufPtr &cbufPtr);

    int set_timer(uint64_t durationMillis, DispatchHandler *handler);

    int set_timer_absolute(boost::xtime expireTime, DispatchHandler *handler);

    int close_socket(struct sockaddr_in &addr);

  private:

    int connect_socket(int sd, struct sockaddr_in &addr, DispatchHandlerPtr &defaultHandlerPtr);

    boost::mutex  m_mutex;
    std::string   m_app_name;
    HandlerMap    m_handler_map;
    Reactor      *m_timer_reactor;
  };

  typedef boost::shared_ptr<Comm> CommPtr;

}

#endif // HYPERTABLE_COMMENGINE_H
