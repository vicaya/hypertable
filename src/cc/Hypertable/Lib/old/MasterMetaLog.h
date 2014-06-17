/** -*- c++ -*-
 * Copyright (C) 2008 Hypertable, Inc.
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

#ifndef HYPERTABLE_MASTER_METALOG_H
#define HYPERTABLE_MASTER_METALOG_H

#include "MetaLogDfsBase.h"
#include "MasterMetaLogReader.h"
#include "MasterMetaLogEntryFactory.h"

namespace Hypertable {

  namespace OldMetaLog {

    class MasterMetaLog : public MetaLogDfsBase {
    public:
      typedef MetaLogDfsBase Parent;

      MasterMetaLog(Filesystem *fs, const String &path);

      /**
       * Purge metalog of old/redundant entries
       *
       * @param range_states - range states to write
       */
      void purge(const ServerStates &server_states);

      /**
       * Recover from existing metalog. Skipping the last bad entry if necessary
       */
      bool recover(const String &path);

      // convenience methods
      void
      log_server_joined(const String &location) {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_master_server_joined(location));
        write(entry.get());
      }

      void
      log_server_left(const String &location) {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_master_server_left(location));
        write(entry.get());
      }

      void
      log_server_removed(const String &location) {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_master_server_removed(location));
        write(entry.get());
      }

      void
      log_range_move_started(const TableIdentifier &table, const RangeSpec &range,
                             const String &transfer_log, uint64_t soft_limit,
                             const String &location) {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_master_range_move_started(
                                                                                 table, range, transfer_log, soft_limit, location));
        write(entry.get());
      }

      void
      log_range_move_restarted(const TableIdentifier &table, const RangeSpec &range,
                               const String &transfer_log, uint64_t soft_limit,
                               const String &location) {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_master_range_move_restarted(
                                                                                   table, range, transfer_log, soft_limit, location));
        write(entry.get());
      }

      void
      log_range_move_loaded(const TableIdentifier &table, const RangeSpec &range,
                            const String &location) {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_master_range_move_loaded(
                                                                                table, range, location));
        write(entry.get());
      }

      void
      log_range_move_acknowledged(const TableIdentifier &table, const RangeSpec &range,
                                  const String &location) {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_master_range_move_acknowledged(
                                                                                      table, range, location));
        write(entry.get());
      }

      void
      log_balance_started() {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_master_balance_started());
        write(entry.get());
      }

      void
      log_balance_done() {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_master_balance_done());
        write(entry.get());
      }

    private:
      void write_header();
    };

    typedef intrusive_ptr<MasterMetaLog> MasterMetaLogPtr;

  } // namespace OldMetaLog

} // namespace Hypertable

#endif // HYPERTABLE_MASTER_METALOG_H
