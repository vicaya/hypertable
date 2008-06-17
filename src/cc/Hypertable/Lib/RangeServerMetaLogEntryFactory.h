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

#ifndef HYPERTABLE_RANGE_SERVER_METALOG_ENTRY_FACTORY_H
#define HYPERTABLE_RANGE_SERVER_METALOG_ENTRY_FACTORY_H

#include <Common/String.h>
#include "Types.h"
#include "RangeServerMetaLog.h"

namespace Hypertable {
namespace MetaLogEntryFactory {

enum RangeServerMetaLogEntryType {
  RS_SPLIT_START        = 1,
  RS_SPLIT_SHRUNK       = 2,
  RS_SPLIT_DONE         = 3,
  RS_RANGE_LOADED       = 4,
  RS_MOVE_START         = 5,
  RS_MOVE_PREPARED      = 6,
  RS_MOVE_DONE          = 7
};

RangeServerMetaLogEntry *
new_rs_split_start(const RangeSpec &old, const RangeSpec &split_off,
                   const String &transfer_log);

RangeServerMetaLogEntry *
new_rs_split_shrunk(const RangeSpec &old);

RangeServerMetaLogEntry *
new_rs_split_done(const RangeSpec &old);

RangeServerMetaLogEntry *
new_rs_range_loaded(const RangeSpec &);

RangeServerMetaLogEntry *
new_rs_move_start(const RangeSpec &, const String &transfer_log);

RangeServerMetaLogEntry *
new_rs_move_prepared(const RangeSpec &);

RangeServerMetaLogEntry *
new_rs_move_done(const RangeSpec &);

RangeServerMetaLogEntry *
new_from_payload(RangeServerMetaLogEntryType, const void *buf, size_t len);

} // namespace MetaLogEntryFactory
} // namespace Hypertable

#endif // HYPERTABLE_RANGE_SERVER_METALOG_ENTRY_FACTORY_H
