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
#include "ScanSpec.h"
#include "Types.h"

namespace Hypertable {

  /** Generates RangeServer protocol request messages */
  class RangeServerProtocol : public Protocol {

  public:
    static const short COMMAND_LOAD_RANGE        = 0;
    static const short COMMAND_UPDATE            = 1;
    static const short COMMAND_CREATE_SCANNER    = 2;
    static const short COMMAND_FETCH_SCANBLOCK   = 3;
    static const short COMMAND_COMPACT           = 4;
    static const short COMMAND_STATUS            = 5;
    static const short COMMAND_SHUTDOWN          = 6;
    static const short COMMAND_DUMP_STATS        = 7;
    static const short COMMAND_DESTROY_SCANNER   = 8;
    static const short COMMAND_DROP_TABLE        = 9;
    static const short COMMAND_DROP_RANGE        = 10;
    static const short COMMAND_REPLAY_BEGIN      = 11;
    static const short COMMAND_REPLAY_LOAD_RANGE = 12;
    static const short COMMAND_REPLAY_UPDATE     = 13;
    static const short COMMAND_REPLAY_COMMIT     = 14;
    static const short COMMAND_GET_STATISTICS    = 15;
    static const short COMMAND_MAX               = 16;

    static const char *m_command_strings[];

    enum RangeGroup {
      GROUP_METADATA_ROOT,
      GROUP_METADATA,
      GROUP_USER
    };

    /** Creates a "load range" request message
     *
     * @param table table identifier
     * @param range range specification
     * @param transfer_log transfer log to replay
     * @param range_state range state
     * @return protocol message
     */
    static CommBuf *create_request_load_range(TableIdentifier &table, RangeSpec &range, const char *transfer_log, RangeState &range_state);

    /** Creates an "update" request message.  The data argument holds a sequence of key/value
     * pairs.  Each key/value pair is encoded as two variable lenght ByteStringrecords
     * back-to-back.  This method transfers ownership of the data buffer to the CommBuf that
     * gets returned.
     *
     * @param table table identifier
     * @param buffer buffer holding key/value pairs
     * @return protocol message
     */
    static CommBuf *create_request_update(TableIdentifier &table, StaticBuffer &buffer);

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

    /** Creates a "replay begin" request message.
     *
     * @param group replay group to begin (METADATA_ROOT, METADATA, USER)
     * @return protocol message
     */
    static CommBuf *create_request_replay_begin(uint16_t group);

    /** Creates a "replay load range" request message.
     *
     * @return protocol message
     */
    static CommBuf *create_request_replay_load_range(TableIdentifier &table, RangeSpec &range, RangeState &range_state);

    /** Creates a "replay update" request message.  The data argument holds a sequence of
     * blocks.  Each block consists of ...
     *
     * @param buffer buffer holding updates to replay
     * @return protocol message
     */
    static CommBuf *create_request_replay_update(StaticBuffer &buffer);

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

    /** Creates a "get statistics" request message.
     *
     * @return protocol message
     */
    static CommBuf *create_request_get_statistics();

    virtual const char *command_text(short command);
  };

}

#endif // HYPERTABLE_RANGESERVERPROTOCOL_H
