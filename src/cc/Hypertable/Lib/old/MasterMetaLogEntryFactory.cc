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

#include "Common/Compat.h"
#include "MasterMetaLogEntries.h"

namespace Hypertable { namespace OldMetaLog { namespace MetaLogEntryFactory {

using namespace MasterTxn;

MetaLogEntry *new_master_server_joined(const String &loc) {
  return new ServerJoined(loc);
}

MetaLogEntry *new_master_server_left(const String &loc) {
  return new ServerLeft(loc);
}

MetaLogEntry *new_master_server_removed(const String &loc) {
  return new ServerRemoved(loc);
}

MetaLogEntry *
new_master_range_move_started(const TableIdentifier &tid, const RangeSpec &rspec,
                              const String &log, uint64_t sl, const String &loc) {
  return new RangeMoveStarted(tid, rspec, log, sl, loc);
}

MetaLogEntry *
new_master_range_move_restarted(const TableIdentifier &tid, const RangeSpec &rspec,
                                const String &log, uint64_t sl, const String &loc) {
  return new RangeMoveRestarted(tid, rspec, log, sl, loc);
}

MetaLogEntry *
new_master_range_move_loaded(const TableIdentifier &tid, const RangeSpec &rspec,
                        const String &loc) {
  return new RangeMoveLoaded(tid, rspec, loc);
}

MetaLogEntry *
new_master_range_move_acknowledged(const TableIdentifier &tid, const RangeSpec &rspec,
                                  const String &loc) {
  return new RangeMoveAcknowledged(tid, rspec, loc);
}

MetaLogEntry *new_master_balance_started() {
  return new BalanceStarted();
}

MetaLogEntry *new_master_balance_done() {
  return new BalanceDone();
}


MetaLogEntry *
new_from_payload(MasterMetaLogEntryType t, int64_t timestamp,
                 StaticBuffer &buf) {
  MetaLogEntry *p = 0;

  switch (t) {
    case MASTER_SERVER_JOINED:            p = new ServerJoined();         break;
    case MASTER_SERVER_LEFT:              p = new ServerLeft();           break;
    case MASTER_SERVER_REMOVED:           p = new ServerRemoved();        break;
    case MASTER_RANGE_MOVE_STARTED :      p = new RangeMoveStarted();     break;

    case MASTER_RANGE_MOVE_LOADED :       p = new RangeMoveLoaded();      break;
    case MASTER_RANGE_MOVE_ACKNOWLEDGED : p = new RangeMoveAcknowledged();break;
    case MASTER_RANGE_MOVE_RESTARTED :    p = new RangeMoveRestarted();   break;
    case MASTER_LOG_RECOVER:              p = new MmlRecover();           break;
    case MASTER_BALANCE_STARTED:          p = new BalanceStarted();       break;
    case MASTER_BALANCE_DONE:             p = new BalanceDone();          break;

    default: HT_THROWF(Error::METALOG_ENTRY_BAD_TYPE, "unknown type (%d)", t);
  }
  p->timestamp = timestamp;
  p->read(buf);
  return p;
}

}}} // namespace Hypertable::MetaLogEntryFactory
