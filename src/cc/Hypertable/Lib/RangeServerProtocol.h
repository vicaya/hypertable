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

#ifndef HYPERTABLE_RANGESERVERPROTOCOL_H
#define HYPERTABLE_RANGESERVERPROTOCOL_H

#include "AsyncComm/Protocol.h"

#include "RangeState.h"
#include "Types.h"

namespace Hypertable {

  /** Generates RangeServer protocol request messages */
  class RangeServerProtocol : public Protocol {

  public:
    static const short COMMAND_LOAD_RANGE       = 0;
    static const short COMMAND_UPDATE           = 1;
    static const short COMMAND_CREATE_SCANNER   = 2;
    static const short COMMAND_FETCH_SCANBLOCK  = 3;
    static const short COMMAND_COMPACT          = 4;
    static const short COMMAND_STATUS           = 5;
    static const short COMMAND_SHUTDOWN         = 6;
    static const short COMMAND_DUMP_STATS       = 7;
    static const short COMMAND_DESTROY_SCANNER  = 8;
    static const short COMMAND_DROP_TABLE       = 9;
    static const short COMMAND_REPLAY_START     = 10;
    static const short COMMAND_REPLAY_UPDATE    = 11;
    static const short COMMAND_REPLAY_COMMIT    = 12;
    static const short COMMAND_DROP_RANGE       = 13;
    static const short COMMAND_MAX              = 14;

    static const uint16_t LOAD_RANGE_FLAG_REPLAY = 0x0001;

    static const char *m_command_strings[];

    /** Creates a "load range" request message
     *
     * @param table table identifier
     * @param range range specification
     * @param transfer_log transfer log to replay
     * @param range_state range state
     * @param flags load flags
     * @return protocol message
     */
    static CommBuf *create_request_load_range(TableIdentifier &table, RangeSpec &range, const char *transfer_log, RangeState &range_state, uint16_t flags);

    /** Creates an "update" request message.  The data argument holds a sequence of key/value
     * pairs.  Each key/value pair is encoded as two variable lenght ByteString32T records
     * back-to-back.  This method transfers ownership of the data buffer to the CommBuf that
     * gets returned.
     *
     * @param table table identifier
     * @param data buffer holding key/value pairs
     * @param len length of data buffer
     * @return protocol message
     */
    static CommBuf *create_request_update(TableIdentifier &table, uint8_t *data, size_t len);

    /** Creates a "create scanner" request message.
     *
     * @param table table identifier
     * @param range range specification
     * @param scan_spec scan specification
     * @return protocol message
     */
    static CommBuf *create_request_create_scanner(TableIdentifier &table, RangeSpec &range, ScanSpec &scan_spec);

    /** Creates a "destroy scanner" request message.
     *
     * @param scanner_id scanner ID returned from a "create scanner" request
     * @return protocol message
     */
    static CommBuf *create_request_destroy_scanner(int scanner_id);

    /** Creates a "fetch scanblock" request message.
     *
     * @param scanner_id scanner ID returned from a "create scanner" request
     * @return protocol message
     */
    static CommBuf *create_request_fetch_scanblock(int scanner_id);

    /** Creates a "status" request message.
     *
     * @return protocol message
     */
    static CommBuf *create_request_status();

    /** Creates a "shutdown" request message.
     *
     * @return protocol message
     */
    static CommBuf *create_request_shutdown();

    /** Creates a "dump stats" command (for testint)
     *
     * @return protocol message
     */
    static CommBuf *create_request_dump_stats();

    /** Creates a "drop table" request message.
     *
     * @param table table identifier
     * @return protocol message
     */
    static CommBuf *create_request_drop_table(TableIdentifier &table);

    /** Creates a "replay start" request message.
     *
     * @return protocol message
     */
    static CommBuf *create_request_replay_start();

    /** Creates a "replay update" request message.  The data argument holds a sequence of 
     * blocks.  Each block consists of ...
     *
     * @param data buffer holding updates to replay
     * @param len length of data buffer
     * @return protocol message
     */
    static CommBuf *create_request_replay_update(const uint8_t *data, size_t len);

    /** Creates a "replay commit" request message.
     *
     * @return protocol message
     */
    static CommBuf *create_request_replay_commit();

    /** Creates a "drop range" request message.
     *
     * @param table table identifier
     * @param range range specification
     * @return protocol message
     */
    static CommBuf *create_request_drop_range(TableIdentifier &table, RangeSpec &range);

    virtual const char *command_text(short command);
  };

}

#endif // HYPERTABLE_RANGESERVERPROTOCOL_H
