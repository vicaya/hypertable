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

#include <boost/thread/mutex.hpp>
#include <boost/thread/xtime.hpp>

extern "C" {
#include <stdint.h>
}

#include "Common/ReferenceCount.h"

#include "DispatchHandler.h"
#include "CommBuf.h"
#include "ConnectionHandlerFactory.h"
#include "HandlerMap.h"

using namespace std;

namespace Hypertable {

  /**
   * Provides communication (message passing) services to an application.
   * There should be only one instance of this class per
   * process and the static method ReactorFactory#initialize must be called
   * prior to constructing this class in order to create the system-wide
   * I/O reactor threads.
   */
  class Comm : public ReferenceCount {

  public:

    Comm();

    ~Comm();

    /**
     * Establishes a TCP connection to the address given by the addr argument
     * and associates with it a default dispatch handler.  CONNECTION_ESTABLISHED
     * and DISCONNECT events are delivered to the default dispatch handler.  The
     * argument addr is used to subsequently refer to the connection.
     *
     * @param addr IP address and port to connect to
     * @param default_handler_ptr smart pointer to default dispatch handler
     * @return Error::OK on success or error code on failure
     */
    int connect(struct sockaddr_in &addr, DispatchHandlerPtr &default_handler_ptr);

    /**
     * Establishes a TCP connection to the address given by the addr argument,
     * binding the local side of the connection to the address given by the
     * local_addr argument.  A default dispatch handler is associated with the
     * connection to receive CONNECTION_ESTABLISHED and DISCONNECT events.  The
     * argument addr is used to subsequently refer to the connection.
     *
     * @param addr IP address and port to connect to
     * @param local_addr Local IP address and port to bind to
     * @param default_handler_ptr smart pointer to default dispatch handler
     * @return Error::OK on success or error code on failure
     */
    int connect(struct sockaddr_in &addr, struct sockaddr_in &local_addr, DispatchHandlerPtr &default_handler_ptr);

    /**
     * Sets an alias for a TCP connection
     *
     * @param addr connection identifier (remote address)
     * @param alias alias connection identifier
     * @return Error::OK on success or error code on failure
     */
    int set_alias(struct sockaddr_in &addr, struct sockaddr_in &alias);

    /**
     * Tells the communication subsystem to listen for connection requests on the
     * address given by the addr argument.  New connections will be assigned
     * dispatch handlers by invoking the get_instance method of the connection
     * handler factory supplied as the chf_ptr argument.  CONNECTION_ESTABLISHED
     * events are logged, but not delivered to the application
     *
     * @param addr IP address and port to listen for connection on
     * @param chf_ptr connection handler factory smart pointer
     * @return Error::OK on success or error code on failure
     */
    int listen(struct sockaddr_in &addr, ConnectionHandlerFactoryPtr &chf_ptr);

    /**
     * Tells the communication subsystem to listen for connection requests on the
     * address given by the addr argument.  New connections will be assigned
     * dispatch handlers by invoking the get_instance method of the connection
     * handler factory supplied as the chf_ptr argument.  CONNECTION_ESTABLISHED
     * events are delivered via the default dispatch handler supplied in the
     * default_handler_ptr argument.
     *
     * @param addr IP address and port to listen for connection on
     * @param chf_ptr connection handler factory smart pointer
     * @param default_handler_ptr smart pointer to default dispatch handler
     * @return Error::OK on success or error code on failure
     */
    int listen(struct sockaddr_in &addr, ConnectionHandlerFactoryPtr &chf_ptr, DispatchHandlerPtr &default_handler_ptr);

    /**
     * Sends a request message over a connection, expecting a response.
     * The connection is specified by the addr argument which is the remote
     * end of the connection.  The request message to send is encapsulated
     * in the cbuf_ptr object (see CommBuf) and should start with a valid header.
     * The dispatch handler given by the response_handler argument will get
     * called back with either a response MESSAGE event or a TIMEOUT event if     
     * no response is received within the number of seconds specified by the
     * timeout argument.
     *
     * <p>If the server at the other end of the connection uses an ApplicationQueue
     * to carry out requests, then the gid field in the header can be used to
     * serialize requests that are destined for the same object.  For example,
     * the following code serializes requests to the same file descriptor:
     *
     * <pre>
     * HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
     * hbuilder.set_group_id(fd);
     * CommBuf *cbuf = new CommBuf(hbuilder, 14);
     * cbuf->AppendShort(COMMAND_READ);
     * cbuf->AppendInt(fd);
     * cbuf->AppendLong(amount);
     * </pre>
     *
     * @param addr connection identifier (remote address)
     * @param timeout number of seconds to wait before delivering TIMEOUT event
     * @param cbuf_ptr request message to send (see CommBuf)
     * @param response_handler pointer to response handler associated with the request
     * @return Error::OK on success or error code on failure
     */
    int send_request(struct sockaddr_in &addr, time_t timeout, CommBufPtr &cbuf_ptr, DispatchHandler *response_handler);

    /**
     * Sends a response message back over a connection.  It is assumed that the
     * id field of the header matches the id field of the request for which this
     * is a response to.  The connection is specified by the addr argument which
     * is the remote end of the connection.  The response message to send is
     * encapsulated in the cbuf_ptr (see CommBuf) object and should start with a
     * valid header.  The following code snippet illustrates how a simple response
     * message gets created to send back to a client in response to a request
     * message:
     *
     * <pre>
     * HeaderBuilder hbuilder;
     * hbuilder.initialize_from_request(request_event_ptr->header);
     * CommBufPtr cbufPtr( new CommBuf(hbuilder, 4) );
     * cbufPtr->append_int(Error::OK);
     * </pre>
     *
     * @param addr connection identifier (remote address)
     * @param cbuf_ptr response message to send (must have valid header with matching request id)
     * @return Error::OK on success or error code on failure
     */
    int send_response(struct sockaddr_in &addr, CommBufPtr &cbuf_ptr);

    /**
     * Obtains the local address of a socket connection.  The connection is
     * identified by the remote address in the addr argument.
     *
     * @param addr connection identifier (remote address)
     * @param local_addr pointer to address structure to hold the returned local address
     * @return Error::OK on success or error code on failure
     */
    int get_local_address(struct sockaddr_in addr, struct sockaddr_in *local_addr);

    /**
     * Creates a local socket for receiving datagrams and assigns a default dispatch
     * handler to handle events on this socket.  This socket can also be used for
     * sending datagrams.  The events delivered for this socket consist of either MESSAGE
     * events or ERROR events.
     *
     * @param addr address of the socket
     * @param handler_ptr default dispatch handler to handle the deliver of events
     * @return Error::OK on success or error code on failure
     */
    int create_datagram_receive_socket(struct sockaddr_in *addr, DispatchHandlerPtr &handler_ptr);

    /**
     * Sends a datagram to a remote address.  The remote address is specified by the addr
     * argument and the local socket address to send it from is specified by the send_addr
     * argument.  The send_addr argument must refer to a socket that was created with a call
     * to <a href="#create_datagram_receive_socket">create_datagram_receive_socket</a>.
     *
     * @param addr remote address to send datagram to
     * @param send_addr local socket address to send from
     * @param cbuf_ptr datagram message with valid header
     * @return Error::OK on success or error code on failure
     */
    int send_datagram(struct sockaddr_in &addr, struct sockaddr_in &send_addr, CommBufPtr &cbuf_ptr);

    /**
     * Sets a timer that will generate a TIMER event after some number of milliseconds
     * have elapsed, specified by the duration_millis argument.  The handler argument
     * represents the dispatch handler to receive the TIMER event.
     *
     * @param duration_millis number of milliseconds to wait
     * @param handler the dispatch handler to receive the TIMER event upon expiration
     * @return Error::OK on success or error code on failure
     */
    int set_timer(uint64_t duration_millis, DispatchHandler *handler);

    /**
     * Sets a timer that will generate a TIMER event at the absolute time specified
     * by the expire_time argument.  The handler argument represents the dispatch
     * handler to receive the TIMER event.
     *
     * @param expire_time number of milliseconds to wait
     * @param handler the dispatch handler to receive the TIMER event upon expiration
     * @return Error::OK on success or error code on failure
     */
    int set_timer_absolute(boost::xtime expire_time, DispatchHandler *handler);

    /**
     * Closes the socket connection specified by the addr argument.  This has the 
     * effect of closing the connection and removing it from the event demultiplexer
     * (e.g epoll).  It also causes all outstanding requests on the connection to
     * get purged.
     *
     * @param addr address of socket connection to close
     * @return Error::OK on success or error code on failure
     */
    int close_socket(struct sockaddr_in &addr);

  private:

    int connect_socket(int sd, struct sockaddr_in &addr, DispatchHandlerPtr &default_handler_ptr);

    static atomic_t ms_next_request_id;

    boost::mutex   m_mutex;
    HandlerMapPtr  m_handler_map_ptr;
    ReactorPtr     m_timer_reactor_ptr;
  };
  typedef boost::intrusive_ptr<Comm> CommPtr;

}

#endif // HYPERTABLE_COMMENGINE_H
