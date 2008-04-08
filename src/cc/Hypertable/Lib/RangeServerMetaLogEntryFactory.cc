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
#include "RangeServerMetaLogEntryFactory.h"

using namespace std;

namespace Hypertable {
namespace MetaLogEntryFactory {

RangeServerMetaLogEntry *
new_rs_split_start(const RangeSpec &old, const RangeSpec &split_off,
                   const String &transfer_log) {
  // TODO 
  return NULL;
}

RangeServerMetaLogEntry *
new_rs_split_shrunk(const RangeSpec &old) {
  // TODO
  return NULL;
}

RangeServerMetaLogEntry *
new_rs_split_done(const RangeSpec &old) {
  // TODO
  return NULL;
}

RangeServerMetaLogEntry *
new_rs_range_loaded(const RangeSpec &) {
  // TODO
  return NULL;
}

RangeServerMetaLogEntry *
new_rs_move_start(const RangeSpec &, const String &transfer_log) {
  // TODO
  return NULL;
}

RangeServerMetaLogEntry *
new_rs_move_prepared(const RangeSpec &) {
  // TODO
  return NULL;
}

RangeServerMetaLogEntry *
new_rs_move_done(const RangeSpec &) {
  // TODO
  return NULL;
}

} // namespace MetaLogEntryFactory
} // namespace Hypertable
