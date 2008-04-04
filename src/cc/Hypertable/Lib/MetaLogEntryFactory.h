/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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

#ifndef HYPERTABLE_METALOG_ENTRY_FACTORY_H
#define HYPERTABLE_METALOG_ENTRY_FACTORY_H

#include <Common/String.h>
#include "Types.h"
#include "MetaLog.h"

namespace Hypertable {

namespace MetaLogEntryFactory {
  
enum MetaLogEntryType {
  UNKNOWN = -1,
  NULL_TEST,
  RS_SPLIT_START,
  RS_SPLIT_SHRUNK,
  RS_SPLIT_DONE,
  RS_RANGE_LOADED,
  RS_MOVE_START,
  RS_MOVE_PREPARED,
  RS_MOVE_DONE,
  M_LOAD_RANGE_START,
  M_LOAD_RANGE_DONE,
  M_MOVE_RANGE_START,
  M_MOVE_RANGE_DONE,
  M_RECOVERY_START,
  M_RECOVERY_DONE
};

MetaLogEntry *new_rs_split_start(const RangeSpec &old,
                                 const RangeSpec &split_off,
                                 const String &transfer_log);
MetaLogEntry *new_rs_split_shrunk(const RangeSpec &old);
MetaLogEntry *new_rs_split_done(const RangeSpec &old);
MetaLogEntry *new_rs_range_loaded(const RangeSpec &);
MetaLogEntry *new_rs_move_start(const RangeSpec &, const String &transfer_log);
MetaLogEntry *new_rs_move_prepared(const RangeSpec &);
MetaLogEntry *new_rs_move_done(const RangeSpec &);
MetaLogEntry *new_m_load_range_start(const RangeSpec &, const String &rs_to,
                                     const String &transfer_log);
MetaLogEntry *new_m_load_range_done(const RangeSpec &);
MetaLogEntry *new_m_move_range_start(const RangeSpec &, const String &rs_from,
                                     const String &rs_to);
MetaLogEntry *new_m_move_range_done(const RangeSpec &);
MetaLogEntry *new_m_recovery_start(const String &rs_from);
MetaLogEntry *new_m_recovery_done(const String &rs_from);

} // namespace MetaLogEntryFactory

} // namespace Hypertable

#endif // HYPERTABLE_METALOG_ENTRY_FACTORY_H
