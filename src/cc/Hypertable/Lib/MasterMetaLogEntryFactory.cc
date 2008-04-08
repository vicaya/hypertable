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

#include "Common/Compat.h"
#include "MasterMetaLogEntryFactory.h"

using namespace std;

namespace Hypertable {
namespace MetaLogEntryFactory {

MasterMetaLogEntry *
new_m_load_range_start(const RangeSpec &, const String &rs_to,
                       const String &transfer_log) {
  // TODO
  return NULL;
}

MasterMetaLogEntry *
new_m_load_range_done(const RangeSpec &) {
  // TODO
  return NULL;
}

MasterMetaLogEntry *
new_m_move_range_start(const RangeSpec &, const String &rs_from,
                       const String &rs_to) {
  // TODO
  return NULL;
}

MasterMetaLogEntry *
new_m_move_range_done(const RangeSpec &) {
  // TODO
  return NULL;
}

MasterMetaLogEntry *
new_m_recovery_start(const String &rs_from) {
  // TODO
  return NULL;
}

MasterMetaLogEntry *
new_m_recovery_done(const String &rs_from) {
  // TODO
  return NULL;
}

MasterMetaLogEntry *
new_from_payload(MasterMetaLogEntryType, const void *buf, size_t len) {
  // TODO
  return NULL;
}

} // namespace MetaLogEntryFactory
} // namespace Hypertable
