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

class RangeServerMetaLog : public MetaLogDfsBase {
public:
  typedef MetaLogDfsBase Parent;

  RangeServerMetaLog(Filesystem *fs, const String &path);

  /**
   * purge metalog of old/redundant entries
   *
   * @param range_states - range states to write
   */
  void purge(const RangeStates &);

  // convenience methods
  void
  log_split_start(const TableIdentifier &table, const RangeSpec &range,
                  const RangeSpec &split_off, const RangeState &state) {
    MetaLogEntryPtr entry(MetaLogEntryFactory::new_rs_split_start(
                          table, range, split_off, state));
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
  log_split_done(const TableIdentifier &table, const RangeSpec &range) {
    MetaLogEntryPtr entry(MetaLogEntryFactory::new_rs_split_done(
                          table, range));
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
  log_move_start(const TableIdentifier &table, const RangeSpec &range,
                 const RangeState &state) {
    MetaLogEntryPtr entry(MetaLogEntryFactory::new_rs_move_start(
                          table, range, state));
    write(entry.get());
  }

  void
  log_move_prepared(const TableIdentifier &table, const RangeSpec &range) {
    MetaLogEntryPtr entry(MetaLogEntryFactory::new_rs_move_prepared(
                          table, range));
    write(entry.get());
  }

  void
  log_move_done(const TableIdentifier &table, const RangeSpec &range) {
    MetaLogEntryPtr entry(MetaLogEntryFactory::new_rs_move_done(table, range));
    write(entry.get());
  }

  void
  log_drop_table(const TableIdentifier &table) {
    MetaLogEntryPtr entry(MetaLogEntryFactory::new_rs_drop_table(table));
    write(entry.get());
  }

};

typedef intrusive_ptr<RangeServerMetaLog> RangeServerMetaLogPtr;

} // namespace Hypertable

#endif // HYPERTABLE_RANGE_SERVER_METALOG_H
