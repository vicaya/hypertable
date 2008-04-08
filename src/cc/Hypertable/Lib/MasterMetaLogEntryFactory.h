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

#ifndef HYPERTABLE_MASTER_METALOG_ENTRY_FACTORY_H
#define HYPERTABLE_MASTER_METALOG_ENTRY_FACTORY_H

#include <Common/String.h>
#include "Types.h"
#include "MasterMetaLog.h"

namespace Hypertable {

namespace MetaLogEntryFactory {
  
enum MasterMetaLogEntryType {
  M_LOAD_RANGE_START    = 101,
  M_LOAD_RANGE_DONE     = 102,
  M_MOVE_RANGE_START    = 103,
  M_MOVE_RANGE_DONE     = 104,
  M_RECOVERY_START      = 105,
  M_RECOVERY_DONE       = 106
};

MasterMetaLogEntry *
new_m_load_range_start(const RangeSpec &, const String &rs_to,
                       const String &transfer_log);

MasterMetaLogEntry *
new_m_load_range_done(const RangeSpec &);

MasterMetaLogEntry *
new_m_move_range_start(const RangeSpec &, const String &rs_from,
                       const String &rs_to);
MasterMetaLogEntry *
new_m_move_range_done(const RangeSpec &);

MasterMetaLogEntry *
new_m_recovery_start(const String &rs_from);

MasterMetaLogEntry *
new_m_recovery_done(const String &rs_from);

MasterMetaLogEntry *
new_from_payload(MasterMetaLogEntryType, const void *buf, size_t len);

} // namespace MetaLogEntryFactory

} // namespace Hypertable

#endif // HYPERTABLE_MASTER_METALOG_ENTRY_FACTORY_H
