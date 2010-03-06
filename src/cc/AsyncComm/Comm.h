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

#ifndef HYPERTABLE_COMMENGINE_H
#define HYPERTABLE_COMMENGINE_H

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"

#include "CommAddress.h"
#include "CommBuf.h"
#include "ConnectionHandlerFactory.h"
#include "DispatchHandler.h"
#include "HandlerMap.h"


namespace Hypertable {

  /**
   * Provides communication (message passing) services to an application.
   * There should be only one instance of this class per process and the static
   * method ReactorFactory#initialize must be called prior to constructing this
   * class in order to create the system-wide I/O reactor threads.
   */
  class Comm : public ReferenceCount {
  public:
    static Comm *instance() {
      ScopedLock lock(ms_mutex);

      if (!ms_instance)
        ms_instance = new Comm();

      return ms_instance;
    }

    static void destroy();

    /**
     * Establishes a TCP connection to the address given by the addr argument
     * and associates with it a default dispatch handler.
     * CONNECTION_ESTABLISHED and DISCONNECT events are delivered to the
     * default dispatch handler.  The argument addr is used to subsequently
     * refer to the connection.
     *
     * @param addr address to connect to
     * @param default_handler smart pointer to default dispatch handler
     * @return Error::OK on success or error code on failure
     */
    int connect(const CommAddress &addr,
                DispatchHandlerPtr &default_handler);

    /**
     * Establishes a TCP connection to the address given by the addr argument,
     * binding the local side of the connection to the address given by the
     * local_addr argument.  A default dispatch handler is associated with the
     * connection to receive CONNECTION_ESTABLISHED and DISCONNECT events.  The
     * argument addr is used to subsequently refer to the connection.
     *
     * @param addr address to connect to
     * @param local_addr Local address to bind to
     * @param default_handler smart pointer to default dispatch handler
     * @return Error::OK on success or error code on failure
     */
    int connect(const CommAddress &addr, const CommAddress &local_addr,
                DispatchHandlerPtr &default_handler);


    /**
     * Sets an alias for a TCP connection
     *
     * @param addr connection identifier (remote address)
     * @param alias alias connection identifier
     */
    int set_alias(const InetAddr &addr, const InetAddr &alias);

    /**
     * Adds a proxy name for a TCP connection
     *
     * @param proxy proxy name
     * @param addr connection identifier (remote address)
     */
    int add_proxy(const String &proxy, const InetAddr &addr);

    /**
     * Fills in the proxy map
     *
     * @param proxy_map reference to proxy map to be filled in
     */
    void get_proxy_map(ProxyMapT &proxy_map);

    /**
     * Tells the communication subsystem to listen for connection requests on
     * the address given by the addr argument.  New connections will be
     * assigned dispatch handlers by invoking the get_instance method of the
     * connection handler factory supplied as the chf argument.
     * CONNECTION_ESTABLISHED events are logged, but not delivered to the
     * application
     *
     * @param addr IP address and port to listen for connection on
     * @param chf connection handler factory smart pointer
     */
    void listen(const CommAddress &addr, ConnectionHandlerFactoryPtr &chf);

    /**
     * Tells the communication subsystem to listen for connection requests on
     * the address given by the addr argument.  New connections will be
     * assigned dispatch handlers by invoking the get_instance method of the
     * connection handler factory supplied as the chf argument.
     * CONNECTION_ESTABLISHED events are delivered via the default dispatch
     * handler supplied in the default_handler argument.
     *
     * @param addr IP address and port to listen for connection on
     * @param chf connection handler factory smart pointer
     * @param default_handler smart pointer to default dispatch handler
     */
    void listen(const CommAddress &addr, ConnectionHandlerFactoryPtr &chf,
                DispatchHandlerPtr &default_handler);

    /**
     * Sends a request message over a connection, expecting a response.  The
     * connection is specified by the addr argument which is the remote end of
     * the connection.  The request message to send is encapsulated in the
     * cbuf object (see CommBuf) and should start with a valid header.  The
     * dispatch handler given by the response_handler argument will get called
     * back with either a response MESSAGE event or a TIMEOUT event if no
     * response is received within the number of seconds specified by the
     * timeout argument.
     *
     * <p>If the server at the other end of the connection uses an
     * ApplicationQueue to carry out requests, then the gid field in the header
     * can be used to serialize requests that are destined for the same object.
     * For example, the following code serializes requests to the same file
     * descriptor:
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
     * @param timeout_ms number of milliseconds to wait before delivering
     *        TIMEOUT event
     * @param cbuf request message to send (see CommBuf)
     * @param response_handler pointer to response handler associated with the
     *        request
     * @return Error::OK on success or error code on failure
     */
    int send_request(const CommAddress &addr, uint32_t timeout_ms,
                     CommBufPtr &cbuf, DispatchHandler *response_handler);

    /**
     * Sends a response message back over a connection.  It is assumed that the
     * id field of the header matches the id field of the request for which
     * this is a response to.  The connection is specified by the addr argument
     * which is the remote end of the connection.  The response message to send
     * is encapsulated in the cbuf (see CommBuf) object and should start
     * with a valid header.  The following code snippet illustrates how a
     * simple response message gets created to send back to a client in
     * response to a request message:
     *
     * <pre>
     * HeaderBuilder hbuilder;
     * hbuilder.initialize_from_request(request_event->header);
     * CommBufPtr cbp(new CommBuf(hbuilder, 4));
     * cbp->append_i32(Error::OK);
     * </pre>
     *
     * @param addr connection identifier (remote address)
     * @param cbuf response message to send (must have valid header with
     *        matching request id)
     * @return Error::OK on success or error code on failure
     */
    int send_response(const CommAddress &addr, CommBufPtr &cbuf);

    /**
     * Obtains the local address of a socket connection.  The connection is
     * identified by the remote address in the addr argument.
     *
     * @param addr connection identifier (remote address)
     * @param local_addr pointer to address structure to hold the returned
     *        local address
     * @return Error::OK on success or error code on failure
     */
    int get_local_address(const CommAddress &addr, CommAddress &local_addr);

    /**
     * Creates a local socket for receiving datagrams and assigns a default
     * dispatch handler to handle events on this socket.  This socket can also
     * be used for sending datagrams.  The events delivered for this socket
     * consist of either MESSAGE events or ERROR events.
     *
     * @param addr pointer to address structure
     * @param tos TOS value to set on IP packet
     * @param handler default dispatch handler to handle the deliver of
     *        events
     */
    void create_datagram_receive_socket(CommAddress &addr, int tos,
                                        DispatchHandlerPtr &handler);

    /**
     * Sends a datagram to a remote address.  The remote address is specified
     * by the addr argument and the local socket address to send it from is
     * specified by the send_addr argument.  The send_addr argument must refer
     * to a socket that was created with a call to
     * #create_datagram_receive_socket.
     *
     * @param addr remote address to send datagram to
     * @param send_addr local socket address to send from
     * @param cbuf datagram message with valid header
     * @return Error::OK on success or error code on failure
     */
    int send_datagram(const CommAddress &addr, const CommAddress &send_addr,
                      CommBufPtr &cbuf);

    /**
     * Sets a timer that will generate a TIMER event after some number of
     * milliseconds have elapsed, specified by the duration_millis argument.
     * The handler argument represents the dispatch handler to receive the
     * TIMER event.
     *
     * @param duration_millis number of milliseconds to wait
     * @param handler the dispatch handler to receive the TIMER event upon
     *        expiration
     * @return Error::OK on success or error code on failure
     */
    int set_timer(uint32_t duration_millis, DispatchHandler *handler);

    /**
     * Sets a timer that will generate a TIMER event at the absolute time
     * specified by the expire_time argument.  The handler argument represents
     * the dispatch handler to receive the TIMER event.
     *
     * @param expire_time number of milliseconds to wait
     * @param handler the dispatch handler to receive the TIMER event upon
     *        expiration
     * @return Error::OK on success or error code on failure
     */
    int set_timer_absolute(boost::xtime expire_time, DispatchHandler *handler);

    /**
     * Closes the socket connection specified by the addr argument.  This has
     * the effect of closing the connection and removing it from the event
     * demultiplexer (e.g epoll).  It also causes all outstanding requests on
     * the connection to get purged.
     *
     * @param addr address of socket connection to close
     * @return Error::OK on success or error code on failure
     */
    int close_socket(const CommAddress &addr);

  private:
    Comm();     // prevent non-singleton usage
    ~Comm();

    static Comm *ms_instance;

    int send_request(IOHandlerDataPtr &data_handler, uint32_t timeout_ms,
                     CommBufPtr &cbuf, DispatchHandler *response_handler);

    int connect_socket(int sd, const CommAddress &addr,
                       DispatchHandlerPtr &default_handler);

    static atomic_t ms_next_request_id;

    static Mutex   ms_mutex;
    HandlerMapPtr  m_handler_map;
    ReactorPtr     m_timer_reactor;
  };

} // namespace Hypertable

#endif // HYPERTABLE_COMMENGINE_H
