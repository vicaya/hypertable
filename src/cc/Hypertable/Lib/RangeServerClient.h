/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#ifndef HYPERTABLE_RANGESERVERCLIENT_H
#define HYPERTABLE_RANGESERVERCLIENT_H

#include <boost/intrusive_ptr.hpp>

#include "Common/Properties.h"
#include "Common/ReferenceCount.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/CommBuf.h"
#include "AsyncComm/DispatchHandler.h"

#include "RangeServerProtocol.h"
#include "RangeState.h"
#include "Types.h"

using namespace Hypertable;

namespace Hypertable {

  class ScanBlock;

  /** Client proxy interface to RangeServer. */
  class RangeServerClient : public ReferenceCount {
  public:

    RangeServerClient(Comm *comm, time_t timeout);
    ~RangeServerClient();

    /** Sets the default client connection timeout
     *
     * @param timeout timeout value in seconds
     */
    void set_default_timeout(time_t timeout) { m_default_timeout = timeout; }

    /** Sets the request timeout
     *
     * @param timeout timeout value in seconds
     */
    void set_timeout(time_t timeout) { m_timeout = timeout; }

    /** Issues a "load range" request asynchronously.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param range range specification
     * @param transfer_log transfer log
     * @param range_state range state
     * @param flags load flags
     * @param handler response handler
     */
    void load_range(struct sockaddr_in &addr, TableIdentifier &table, RangeSpec &range, const char *transfer_log, RangeState &range_state, uint16_t flags, DispatchHandler *handler);

    /** Issues a "load range" request.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param range range specification
     * @param transfer_log transfer log
     * @param range_state range state
     * @param flags load flags
     */
    void load_range(struct sockaddr_in &addr, TableIdentifier &table, RangeSpec &range, const char *transfer_log, RangeState &range_state, uint16_t flags);

    /** Issues an "update" request asynchronously.  The data argument holds a sequence of key/value
     * pairs.  Each key/value pair is encoded as two variable lenght ByteString32T records
     * back-to-back.  This method takes ownership of the data buffer.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param data buffer holding key/value pairs
     * @param len length of data buffer
     * @param handler response handler
     */
    void update(struct sockaddr_in &addr, TableIdentifier &table, uint8_t *data, size_t len, DispatchHandler *handler);

    /** Issues an "update" request.  The data argument holds a sequence of key/value
     * pairs.  Each key/value pair is encoded as two variable lenght ByteString32T records
     * back-to-back.  This method takes ownership of the data buffer.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param data buffer holding key/value pairs
     * @param len length of data buffer
     */
    void update(struct sockaddr_in &addr, TableIdentifier &table, uint8_t *data, size_t len);

    /** Issues a "create scanner" request asynchronously.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param range range specification
     * @param scan_spec scan specification
     * @param handler response handler
     */
    void create_scanner(struct sockaddr_in &addr, TableIdentifier &table, RangeSpec &range, ScanSpec &scan_spec, DispatchHandler *handler);

    /** Issues a "create scanner" request.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param range range specification
     * @param scan_spec scan specification
     * @param scan_block block of return key/value pairs
     */
    void create_scanner(struct sockaddr_in &addr, TableIdentifier &table, RangeSpec &range, ScanSpec &scan_spec, ScanBlock &scan_block);

    /** Issues a "destroy scanner" request asynchronously.
     *
     * @param addr remote address of RangeServer connection
     * @param scanner_id Scanner ID returned from a call to create_scanner.
     * @param handler response handler
     */
    void destroy_scanner(struct sockaddr_in &addr, int scanner_id, DispatchHandler *handler);

    /** Issues a "destroy scanner" request.
     *
     * @param addr remote address of RangeServer connection
     * @param scanner_id scanner ID returned from a call to create_scanner.
     */
    void destroy_scanner(struct sockaddr_in &addr, int scanner_id);

    /** Issues a "fetch scanblock" request asynchronously.
     *
     * @param addr remote address of RangeServer connection
     * @param scanner_id Scanner ID returned from a call to create_scanner.
     * @param handler response handler
     */
    void fetch_scanblock(struct sockaddr_in &addr, int scanner_id, DispatchHandler *handler);

    /** Issues a "fetch scanblock" request.
     *
     * @param addr remote address of RangeServer connection
     * @param scanner_id scanner ID returned from a call to create_scanner.
     * @param scan_block block of return key/value pairs
     */
    void fetch_scanblock(struct sockaddr_in &addr, int scanner_id, ScanBlock &scan_block);

    /** Issues a "drop table" request asynchronously.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param handler response handler
     */
    void drop_table(struct sockaddr_in &addr, TableIdentifier &table, DispatchHandler *handler);

    /** Issues a "drop table" request.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     */
    void drop_table(struct sockaddr_in &addr, TableIdentifier &table);

    /** Issues a "status" request.  This call blocks until it receives a response from the server.
     * 
     * @param addr remote address of RangeServer connection
     */
    void status(struct sockaddr_in &addr);

    /** Issues a "shutdown" request.  This call blocks until it receives a response from the
     * server or times out.
     * 
     * @param addr remote address of RangeServer connection
     */
    void shutdown(struct sockaddr_in &addr);

    void dump_stats(struct sockaddr_in &addr);

    /** Issues a "replay start" request.
     *
     * @param addr remote address of RangeServer connection
     * @param handler response handler
     */
    void replay_start(struct sockaddr_in &addr, DispatchHandler *handler);

    /** Issues a "replay update" request.
     *
     * @param addr remote address of RangeServer connection
     * @param data buffer holding replay updates
     * @param len length of data buffer
     * @param handler response handler
     */
    void replay_update(struct sockaddr_in &addr, const uint8_t *data, size_t len, DispatchHandler *handler);

    /** Issues a "replay commit" request.
     *
     * @param addr remote address of RangeServer connection
     * @param handler response handler
     */
    void replay_commit(struct sockaddr_in &addr, DispatchHandler *handler);

    /** Issues a "load range" request asynchronously.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param range range specification
     * @param handler response handler
     */
    void drop_range(struct sockaddr_in &addr, TableIdentifier &table, RangeSpec &range, DispatchHandler *handler);

  private:

    void send_message(struct sockaddr_in &addr, CommBufPtr &cbufPtr, DispatchHandler *handler);
  
    Comm *m_comm;
    time_t m_default_timeout;
    time_t m_timeout;
  };

  typedef boost::intrusive_ptr<RangeServerClient> RangeServerClientPtr;
  

}

#endif // HYPERTABLE_RANGESERVERCLIENT_H
