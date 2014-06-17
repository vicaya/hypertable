/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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

#ifndef HYPERTABLE_RANGE_SERVER_METALOG_H
#define HYPERTABLE_RANGE_SERVER_METALOG_H

#include "MetaLogDfsBase.h"
#include "RangeServerMetaLogReader.h"
#include "RangeServerMetaLogEntryFactory.h"

namespace Hypertable {
  namespace OldMetaLog {

    class RangeServerMetaLog : public MetaLogDfsBase {
    public:
      typedef MetaLogDfsBase Parent;

      RangeServerMetaLog(Filesystem *fs, const String &path);

      /**
       * Purge metalog of old/redundant entries
       *
       * @param range_states - range states to write
       */
      void purge(const RangeStates &range_states);

      /**
       * Recover from existing metalog. Skipping the last bad entry if necessary
       */
      bool recover(const String &path);

      // convenience methods
      void
      log_split_start(const TableIdentifier &table, const RangeSpec &range,
                      const RangeState &state) {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_rs_split_start(
                                                                      table, range, state));
        write(entry.get());
      }

      void
      log_split_shrunk(const TableIdentifier &table, const RangeSpec &range,
                       const RangeState &state) {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_rs_split_shrunk(
                                                                       table, range, state));
        write(entry.get());
      }

      void
      log_split_done(const TableIdentifier &table, const RangeSpec &range,
                     const RangeState &state) {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_rs_split_done(
                                                                     table, range, state));
        write(entry.get());
      }

      void
      log_range_loaded(const TableIdentifier &table, const RangeSpec &range,
                       const RangeState &state) {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_rs_range_loaded(
                                                                       table, range, state));
        write(entry.get());
      }

      void
      log_relinquish_start(const TableIdentifier &table, const RangeSpec &range,
                           const RangeState &state) {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_rs_relinquish_start(
                                                                           table, range, state));
        write(entry.get());
      }

      void
      log_relinquish_done(const TableIdentifier &table, const RangeSpec &range) {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_rs_relinquish_done(table, range));
        write(entry.get());
      }

      void
      log_drop_table(const TableIdentifier &table) {
        MetaLogEntryPtr entry(MetaLogEntryFactory::new_rs_drop_table(table));
        write(entry.get());
      }

    private:
      void write_header();
    };

    typedef intrusive_ptr<RangeServerMetaLog> RangeServerMetaLogPtr;

  } // namespace OldMetaLog

} // namespace Hypertable

#endif // HYPERTABLE_RANGE_SERVER_METALOG_H
