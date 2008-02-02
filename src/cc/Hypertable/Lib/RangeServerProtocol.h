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
#ifndef HYPERTABLE_RANGESERVERPROTOCOL_H
#define HYPERTABLE_RANGESERVERPROTOCOL_H

#include "AsyncComm/Protocol.h"

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
    static const short COMMAND_MAX              = 10;

    static const uint16_t LOAD_RANGE_FLAG_PHANTOM = 0x0001;

    static const char *m_command_strings[];

    /** Creates a "load range" request message
     *
     * @param table table identifier
     * @param range range specification
     * @param soft_limit soft maximum size of range in bytes (doubles at each split up to a max)
     * @param flags load flags
     * @return protocol message
     */
    static CommBuf *create_request_load_range(TableIdentifierT &table, RangeT &range, uint64_t soft_limit, uint16_t flags);

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
    static CommBuf *create_request_update(TableIdentifierT &table, uint8_t *data, size_t len);

    /** Creates a "create scanner" request message.
     *
     * @param table table identifier
     * @param range range specification
     * @param scan_spec scan specification
     * @return protocol message
     */
    static CommBuf *create_request_create_scanner(TableIdentifierT &table, RangeT &range, ScanSpecificationT &scan_spec);

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
     * @param table_name name of table to drop
     * @return protocol message
     */
    static CommBuf *create_request_drop_table(std::string &table_name);

    virtual const char *command_text(short command);
  };

}

#endif // HYPERTABLE_RANGESERVERPROTOCOL_H
