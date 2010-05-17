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

#ifndef HYPERTABLE_RANGESERVERPROTOCOL_H
#define HYPERTABLE_RANGESERVERPROTOCOL_H

#include "AsyncComm/Protocol.h"

#include "Common/StaticBuffer.h"

#include "RangeState.h"
#include "ScanSpec.h"
#include "Types.h"

namespace Hypertable {

  /**
   * Monitoring stats. The labels M0 and ML in the variable name indicates
   * that missing values mean 0 and last known value respectively.
   */
  namespace MonitoringStatsV0 {
    static const uint8_t ML_SYS_DISK_AVAILABLE           = 0;
    static const uint8_t ML_SYS_DISK_USED                = 1;
    static const uint8_t M0_SYS_DISK_READ_KBPS           = 2;
    static const uint8_t M0_SYS_DISK_WRITE_KBPS          = 3;
    static const uint8_t M0_SYS_DISK_READ_RATE           = 4;
    static const uint8_t M0_SYS_DISK_WRITE_RATE          = 5;
    static const uint8_t ML_SYS_MEMORY_TOTAL             = 6;
    static const uint8_t ML_SYS_MEMORY_USED              = 7;
    static const uint8_t ML_VM_SIZE                      = 8;
    static const uint8_t ML_VM_RESIDENT                  = 9;
    static const uint8_t M0_SYS_NET_RECV_KBPS            = 10;
    static const uint8_t M0_SYS_NET_SEND_KBPS            = 11;
    static const uint8_t M0_SYS_LOADAVG_0                = 12;
    static const uint8_t M0_SYS_LOADAVG_1                = 13;
    static const uint8_t M0_SYS_LOADAVG_2                = 14;
    static const uint8_t ML_CPU_PCT                      = 15;
    static const uint8_t ML_SYS_NUM_CORES                = 16;
    static const uint8_t ML_SYS_CLOCK_MHZ                = 17;

    static const uint8_t M0_SCAN_CREATES                 = 18;//create_scanner reqs
    static const uint8_t M0_SCAN_CELLS_RETURNED          = 19;//returned cells
    static const uint8_t M0_SCAN_BYTES                   = 20;//Bytes scanned
    static const uint8_t M0_UPDATES                      = 21;//#updates
    static const uint8_t M0_UPDATE_CELLS                 = 22;//#cells updated
    static const uint8_t M0_UPDATE_BYTES                 = 23;//Bytes updated
    static const uint8_t M0_SYNCS                        = 24;//#updates with sync
    static const uint8_t ML_DISK_USED                    = 25;
    static const uint8_t ML_MEMORY_USED                  = 26;
    static const uint8_t ML_MEMORY_ALLOCATED             = 27;
    static const uint8_t ML_BLOCK_CACHE_MAX_MEMORY       = 28;
    static const uint8_t ML_BLOCK_CACHE_AVAILABLE_MEMORY = 29;
    static const uint8_t M0_BLOCK_CACHE_ACCESSES         = 30;
    static const uint8_t M0_BLOCK_CACHE_HITS             = 31;
    static const uint8_t ML_QUERY_CACHE_MAX_MEMORY       = 32;
    static const uint8_t ML_QUERY_CACHE_AVAILABLE_MEMORY = 33;
    static const uint8_t M0_QUERY_CACHE_ACCESSES         = 34;
    static const uint8_t M0_QUERY_CACHE_HITS             = 35;
    static const uint8_t ML_BLOOM_FILTER_SIZE            = 36;
    static const uint8_t M0_BLOOM_FILTER_ACCESSES        = 37;
    static const uint8_t M0_BLOOM_FILTER_MAYBES          = 38;
    static const uint8_t M0_BLOOM_FILTER_FALSE_POSITIVES = 39;
    static const uint8_t ML_BLOCK_INDEX_SIZE             = 40;
    static const uint8_t ML_SHADOW_CACHE_SIZE            = 41;
    static const uint8_t M0_SHADOW_CACHE_ACCESSES        = 42;
    static const uint8_t M0_SHADOW_CACHE_HITS            = 43;
    static const uint8_t ML_ROOT_COMMIT_LOG_SIZE         = 44;
    static const uint8_t ML_META_COMMIT_LOG_SIZE         = 45;
    static const uint8_t ML_USER_COMMIT_LOG_SIZE         = 46;
  } // MonitoringStatsV0

  /** Generates RangeServer protocol request messages */
  class RangeServerProtocol : public Protocol {

  public:
    static const uint64_t COMMAND_LOAD_RANGE        = 0;
    static const uint64_t COMMAND_UPDATE            = 1;
    static const uint64_t COMMAND_CREATE_SCANNER    = 2;
    static const uint64_t COMMAND_FETCH_SCANBLOCK   = 3;
    static const uint64_t COMMAND_COMPACT           = 4;
    static const uint64_t COMMAND_STATUS            = 5;
    static const uint64_t COMMAND_SHUTDOWN          = 6;
    static const uint64_t COMMAND_DUMP              = 7;
    static const uint64_t COMMAND_DESTROY_SCANNER   = 8;
    static const uint64_t COMMAND_DROP_TABLE        = 9;
    static const uint64_t COMMAND_DROP_RANGE        = 10;
    static const uint64_t COMMAND_REPLAY_BEGIN      = 11;
    static const uint64_t COMMAND_REPLAY_LOAD_RANGE = 12;
    static const uint64_t COMMAND_REPLAY_UPDATE     = 13;
    static const uint64_t COMMAND_REPLAY_COMMIT     = 14;
    static const uint64_t COMMAND_GET_STATISTICS    = 15;
    static const uint64_t COMMAND_UPDATE_SCHEMA     = 16;
    static const uint64_t COMMAND_COMMIT_LOG_SYNC   = 17;
    static const uint64_t COMMAND_CLOSE             = 18;
    static const uint64_t COMMAND_MAX               = 19;

    static const char *m_command_strings[];

    enum RangeGroup {
      GROUP_METADATA_ROOT,
      GROUP_METADATA,
      GROUP_USER
    };

    // The flags shd be the same as in Hypertable::TableMutator.
    enum {
      /* Don't force a commit log sync on update */
      UPDATE_FLAG_NO_LOG_SYNC        = 0x0001,
      UPDATE_FLAG_IGNORE_UNKNOWN_CFS = 0x0002
    };

    /** Creates a "load range" request message
     *
     * @param table table identifier
     * @param range range specification
     * @param transfer_log transfer log to replay
     * @param range_state range state
     * @return protocol message
     */
    static CommBuf *create_request_load_range(const TableIdentifier &table,
        const RangeSpec &range, const char *transfer_log,
        const RangeState &range_state);

    /** Creates an "update" request message.  The data argument holds a
     * sequence of key/value pairs.  Each key/value pair is encoded as two
     * variable lenght ByteStringrecords back-to-back.  This method transfers
     * ownership of the data buffer to the CommBuf that gets returned.
     *
     * @param table table identifier
     * @param count number of key/value pairs in buffer
     * @param buffer buffer holding key/value pairs
     * @param flags update flags
     * @return protocol message
     */
    static CommBuf *create_request_update(const TableIdentifier &table,
                                          uint32_t count, StaticBuffer &buffer, uint32_t flags);

    /** Creates an "update schema" message. Used to update schema for a
     * table
     *
     * @param table table identifier
     * @param schema the new schema
     * @return protocol message
     */
    static CommBuf *create_request_update_schema(
        const TableIdentifier &table, const char *schema);

    /** Creates an "commit_log_sync" message. Used to make previous range server updates
     * are syncd to the commit log
     *
     * @return protocol message
     */
    static CommBuf *create_request_commit_log_sync();

    /** Creates a "create scanner" request message.
     *
     * @param table table identifier
     * @param range range specification
     * @param scan_spec scan specification
     * @return protocol message
     */
    static CommBuf *create_request_create_scanner(const TableIdentifier &table,
        const RangeSpec &range, const ScanSpec &scan_spec);

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

    /** Creates a "close" request message.
     *
     * @return protocol message
     */
    static CommBuf *create_request_close();

    /** Creates a "shutdown" request message.
     *
     * @return protocol message
     */
    static CommBuf *create_request_shutdown();

    /** Creates a "dump" command (for testing)
     *
     * @param label controls what to dump
     * @param outfile name of file to dump to
     * @return protocol message
     */
    static CommBuf *create_request_dump(const String &outfile,
					bool nokeys);

    /** Creates a "drop table" request message.
     *
     * @param table table identifier
     * @return protocol message
     */
    static CommBuf *create_request_drop_table(const TableIdentifier &table);

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
    static CommBuf *create_request_replay_load_range(const TableIdentifier &,
        const RangeSpec &range, const RangeState &range_state);

    /** Creates a "replay update" request message.  The data argument holds a
     * sequence of blocks.  Each block consists of ...
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
    static CommBuf *create_request_drop_range(const TableIdentifier &table,
                                              const RangeSpec &range);

    /** Creates a "get statistics" request message.
     *
     * @param all_range_stats return all stats for all ranges if true, ow return
     *     only data which has changed since the last snapshot
     * @param snapshot save a snapshot of the latest stats on the RangeServer
     * @return protocol message
     *
     */
    static CommBuf *create_request_get_statistics(bool all_range_stats, bool snapshot);

    virtual const char *command_text(uint64_t command);
  };

}

#endif // HYPERTABLE_RANGESERVERPROTOCOL_H
