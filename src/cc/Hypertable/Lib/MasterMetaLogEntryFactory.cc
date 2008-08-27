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

#include "Common/Compat.h"
#include "MasterMetaLogEntryFactory.h"

using namespace std;

namespace Hypertable {
namespace MetaLogEntryFactory {

MetaLogEntry *
new_m_load_range_start(const TableIdentifier &, const RangeSpec &,
                       const RangeState &, const char *rs_to) {
  // TODO
  return NULL;
}

MetaLogEntry *
new_m_load_range_done(const TableIdentifier &, const RangeSpec &) {
  // TODO
  return NULL;
}

MetaLogEntry *
new_m_move_range_start(const TableIdentifier &, const RangeSpec &,
                       const RangeState &, const char *rs_from,
                       const char * rs_to) {
  // TODO
  return NULL;
}

MetaLogEntry *
new_m_move_range_done(const TableIdentifier &, const RangeSpec &) {
  // TODO
  return NULL;
}

MetaLogEntry *
new_m_recovery_start(const char *rs_from) {
  // TODO
  return NULL;
}

MetaLogEntry *
new_m_recovery_done(const char *rs_from) {
  // TODO
  return NULL;
}

MetaLogEntry *
new_from_payload(MasterMetaLogEntryType, uint64_t ts, StaticBuffer &) {
  // TODO
  return NULL;
}

} // namespace MetaLogEntryFactory
} // namespace Hypertable
