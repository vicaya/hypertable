/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#include "Common/InetAddr.h"
#include "Common/StaticBuffer.h"
#include "Common/ReferenceCount.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/CommBuf.h"
#include "AsyncComm/DispatchHandler.h"

#include "RangeServerProtocol.h"
#include "RangeState.h"
#include "Types.h"
#include "Stat.h"


namespace Hypertable {

  class ScanBlock;

  /** Client proxy interface to RangeServer. */
  class RangeServerClient : public ReferenceCount {
  public:

    RangeServerClient(Comm *comm, uint32_t timeout_ms=0);
    ~RangeServerClient();

    /** Sets the default client connection timeout
     *
     * @param timeout_ms timeout value in milliseconds
     */
    void set_default_timeout(uint32_t timeout_ms) {
      m_default_timeout_ms = timeout_ms;
    }
    uint32_t default_timeout() const { return m_default_timeout_ms; }

    /** Sets the request timeout
     *
     * @param timeout_ms timeout value in milliseconds
     */
    void set_timeout(uint32_t timeout_ms) { m_timeout_ms = timeout_ms; }
    int32_t timeout() const { return m_timeout_ms; }

    /** Issues a "load range" request asynchronously.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param range range specification
     * @param transfer_log transfer log
     * @param range_state range state
     * @param handler response handler
     */
    void load_range(const sockaddr_in &addr, const TableIdentifier &table,
                    const RangeSpec &range, const char *transfer_log,
                    const RangeState &range_state, DispatchHandler *handler);

    /** Issues a "load range" request.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param range range specification
     * @param transfer_log transfer log
     * @param range_state range state
     */
    void load_range(const sockaddr_in &addr, const TableIdentifier &table,
                    const RangeSpec &range, const char *transfer_log,
                    const RangeState &range_state);

    /** Issues an "update" request asynchronously.  The data argument holds a
     * sequence of key/value pairs.  Each key/value pair is encoded as two
     * variable lenght ByteString records back-to-back.  This method takes
     * ownership of the data buffer.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param count number of key/value pairs in buffer
     * @param buffer buffer holding key/value pairs
     * @param flags update flags
     * @param handler response handler
     */
    void update(const sockaddr_in &addr, const TableIdentifier &table,
                uint32_t count, StaticBuffer &buffer, uint32_t flags,
                DispatchHandler *handler);

    /** Issues an "update" request.  The data argument holds a sequence of
     * key/value pairs.  Each key/value pair is encoded as two variable lenght
     * ByteString records back-to-back.  This method takes ownership of the
     * data buffer.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param count number of key/value pairs in buffer
     * @param buffer buffer holding key/value pairs
     * @param flags update flags
     */
    void update(const sockaddr_in &addr, const TableIdentifier &table,
                uint32_t count, StaticBuffer &buffer, uint32_t flags);

    /** Issues a "create scanner" request asynchronously.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param range range specification
     * @param scan_spec scan specification
     * @param handler response handler
     */
    void create_scanner(const sockaddr_in &addr, const TableIdentifier &table,
                        const RangeSpec &range, const ScanSpec &scan_spec,
                        DispatchHandler *handler);

    /** Issues a "create scanner" request.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param range range specification
     * @param scan_spec scan specification
     * @param scan_block block of return key/value pairs
     */
    void create_scanner(const sockaddr_in &addr, const TableIdentifier &table,
                        const RangeSpec &range, const ScanSpec &scan_spec,
                        ScanBlock &scan_block);

    /** Issues a "destroy scanner" request asynchronously.
     *
     * @param addr remote address of RangeServer connection
     * @param scanner_id Scanner ID returned from a call to create_scanner.
     * @param handler response handler
     */
    void destroy_scanner(const sockaddr_in &addr, int scanner_id,
                         DispatchHandler *handler);

    /** Issues a "destroy scanner" request.
     *
     * @param addr remote address of RangeServer connection
     * @param scanner_id scanner ID returned from a call to create_scanner.
     */
    void destroy_scanner(const sockaddr_in &addr, int scanner_id);

    /** Issues a "fetch scanblock" request asynchronously.
     *
     * @param addr remote address of RangeServer connection
     * @param scanner_id Scanner ID returned from a call to create_scanner.
     * @param handler response handler
     */
    void fetch_scanblock(const sockaddr_in &addr, int scanner_id,
                         DispatchHandler *handler);

    /** Issues a "fetch scanblock" request.
     *
     * @param addr remote address of RangeServer connection
     * @param scanner_id scanner ID returned from a call to create_scanner.
     * @param scan_block block of return key/value pairs
     */
    void fetch_scanblock(const sockaddr_in &addr, int scanner_id,
                         ScanBlock &scan_block);

    /** Issues a "drop table" request asynchronously.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param handler response handler
     */
    void drop_table(const sockaddr_in &addr, const TableIdentifier &table,
                    DispatchHandler *handler);

    /** Issues a "drop table" request.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     */
    void drop_table(const sockaddr_in &addr, const TableIdentifier &table);

    /** Issues a "update schema" request asynchronously.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param schema the new schema for this table
     * @param handler response handler
     */
    void update_schema(const sockaddr_in &addr,
        const TableIdentifier &table, const char *schema,
        DispatchHandler *handler);

    /** Issues a "commit_log_sync" request asynchronously.
     *
     * @param addr remote address of RangeServer connection
     * @param handler response handler
     */
    void commit_log_sync(const sockaddr_in &addr, DispatchHandler *handler);

    /** Issues a "status" request.  This call blocks until it receives a
     * response from the server.
     *
     * @param addr remote address of RangeServer connection
     */
    void status(const sockaddr_in &addr);

    /** Issues a "close" request.  This call blocks until it receives a
     * response from the server or times out.
     *
     * @param addr remote address of RangeServer connection
     */
    void close(const sockaddr_in &addr);

    /** Issues a "shutdown" request.  This call blocks until it receives a
     * response from the server or times out.
     *
     * @param addr remote address of RangeServer connection
     */
    void shutdown(const sockaddr_in &addr);

    void dump_stats(const sockaddr_in &addr);

    /** Issues a "get_statistics" request.  This call blocks until it receives a
     * response from the server or times out.
     *
     * @param addr remote address of RangeServer connection
     * @param stat reference to statistics object result
     */
    void get_statistics(const sockaddr_in &addr, RangeServerStat &stat);

    /** Issues a "replay begin" request.
     *
     * @param addr remote address of RangeServer connection
     * @param group replay group to begin (METADATA_ROOT, METADATA, USER)
     * @param handler response handler
     */
    void replay_begin(const sockaddr_in &addr, uint16_t group,
                      DispatchHandler *handler);

    /** Issues a "replay load range" request.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param range range specification
     * @param state range state object
     * @param handler response handler
     */
    void replay_load_range(const sockaddr_in &addr,
                           const TableIdentifier &table,
                           const RangeSpec &range, const RangeState &state,
                           DispatchHandler *handler);

    /** Issues a "replay update" request.
     *
     * @param addr remote address of RangeServer connection
     * @param buffer buffer holding replay updates
     * @param handler response handler
     */
    void replay_update(const sockaddr_in &addr, StaticBuffer &buffer,
                       DispatchHandler *handler);

    /** Issues a "replay commit" request.
     *
     * @param addr remote address of RangeServer connection
     * @param handler response handler
     */
    void replay_commit(const sockaddr_in &addr, DispatchHandler *handler);

    /** Issues a "load range" request asynchronously.
     *
     * @param addr remote address of RangeServer connection
     * @param table table identifier
     * @param range range specification
     * @param handler response handler
     */
    void drop_range(const sockaddr_in &addr, const TableIdentifier &table,
                    const RangeSpec &range, DispatchHandler *handler);

  private:

    void send_message(const sockaddr_in &addr, CommBufPtr &cbp,
                      DispatchHandler *handler);

    Comm *m_comm;
    uint32_t m_default_timeout_ms;
    uint32_t m_timeout_ms;
  };

  typedef boost::intrusive_ptr<RangeServerClient> RangeServerClientPtr;

} // namespace Hypertable

#endif // HYPERTABLE_RANGESERVERCLIENT_H
