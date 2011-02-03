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

#ifndef HYPERTABLE_MASTER_METALOG_ENTRY_FACTORY_H
#define HYPERTABLE_MASTER_METALOG_ENTRY_FACTORY_H

#include <Common/String.h>
#include "Hypertable/Lib/Types.h"
#include "Hypertable/Lib/RangeState.h"

#include "MetaLog.h"

namespace Hypertable { namespace OldMetaLog { namespace MetaLogEntryFactory {

enum MasterMetaLogEntryType {
  MASTER_SERVER_JOINED              = 101,
  MASTER_SERVER_LEFT                = 102,
  MASTER_SERVER_REMOVED             = 103,
  MASTER_RANGE_MOVE_STARTED         = 104,
  MASTER_RANGE_MOVE_LOADED          = 105,
  MASTER_RANGE_MOVE_ACKNOWLEDGED    = 106,
  MASTER_RANGE_MOVE_RESTARTED       = 107,
  MASTER_LOG_RECOVER                = 108,
  MASTER_BALANCE_STARTED            = 109,
  MASTER_BALANCE_DONE               = 110
};

MetaLogEntry *new_master_server_joined(const String &loc);

MetaLogEntry *new_master_server_left(const String &loc);

MetaLogEntry *new_master_server_removed(const String &loc);

MetaLogEntry *
new_master_range_move_started(const TableIdentifier &tid, const RangeSpec &rspec,
                              const String &log, uint64_t sl, const String &loc);
MetaLogEntry *
new_master_range_move_restarted(const TableIdentifier &tid, const RangeSpec &rspec,
                                const String &log, uint64_t sl, const String &loc);

MetaLogEntry * new_master_range_move_loaded(const TableIdentifier &tid, const RangeSpec &rspec,
                                            const String &loc);

MetaLogEntry *
new_master_range_move_acknowledged(const TableIdentifier &tid, const RangeSpec &rspec,
                                   const String &loc);

MetaLogEntry *new_master_balance_started();

MetaLogEntry *new_master_balance_done();

MetaLogEntry *
new_from_payload(MasterMetaLogEntryType, int64_t timestamp,
                 StaticBuffer &);

}}} // namespace Hypertable::MetaLogEntryFactory

#endif // HYPERTABLE_MASTER_METALOG_ENTRY_FACTORY_H
