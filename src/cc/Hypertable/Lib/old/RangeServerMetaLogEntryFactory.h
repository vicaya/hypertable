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

#ifndef HYPERTABLE_RANGE_SERVER_METALOG_ENTRY_FACTORY_H
#define HYPERTABLE_RANGE_SERVER_METALOG_ENTRY_FACTORY_H

#include <Common/String.h>

#include "Hypertable/Lib/Types.h"
#include "Hypertable/Lib/RangeState.h"

#include "MetaLog.h"

namespace Hypertable { namespace OldMetaLog { namespace MetaLogEntryFactory {

enum RangeServerMetaLogEntryType {
  RS_SPLIT_START        = 1,
  RS_SPLIT_SHRUNK       = 2,
  RS_SPLIT_DONE         = 3,
  RS_RANGE_LOADED       = 4,
  RS_RELINQUISH_START   = 5,
  RS_MOVE_PREPARED      = 6,
  RS_RELINQUISH_DONE    = 7,
  RS_DROP_TABLE         = 8,
  RS_LOG_RECOVER        = 9
};

MetaLogEntry *
new_rs_split_start(const TableIdentifier &, const RangeSpec &old,
                   const RangeState &);

MetaLogEntry *
new_rs_split_shrunk(const TableIdentifier &, const RangeSpec &old,
                    const RangeState &);

MetaLogEntry *
new_rs_split_done(const TableIdentifier &, const RangeSpec &old,
                  const RangeState &);

MetaLogEntry *
new_rs_range_loaded(const TableIdentifier &, const RangeSpec &,
                    const RangeState &);
MetaLogEntry *
new_rs_relinquish_start(const TableIdentifier &, const RangeSpec &old,
                        const RangeState &);

MetaLogEntry *
new_rs_relinquish_done(const TableIdentifier &, const RangeSpec &old);

MetaLogEntry *
new_rs_drop_table(const TableIdentifier &);

MetaLogEntry *
new_from_payload(RangeServerMetaLogEntryType, int64_t timestamp,
                 StaticBuffer &);

}}} // namespace Hypertable::MetaLogEntryFactory

#endif // HYPERTABLE_RANGE_SERVER_METALOG_ENTRY_FACTORY_H
