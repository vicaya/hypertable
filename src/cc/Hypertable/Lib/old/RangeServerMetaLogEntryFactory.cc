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
#include "RangeServerMetaLogEntries.h"

namespace Hypertable { namespace OldMetaLog { namespace MetaLogEntryFactory {

using namespace RangeServerTxn;

MetaLogEntry *
new_rs_split_start(const TableIdentifier &table, const RangeSpec &old,
                   const RangeState &state) {
  return new SplitStart(table, old, state);
}

MetaLogEntry *
new_rs_split_shrunk(const TableIdentifier &table, const RangeSpec &old,
                    const RangeState &state) {
  return new SplitShrunk(table, old, state);
}

MetaLogEntry *
new_rs_split_done(const TableIdentifier &table, const RangeSpec &old,
                  const RangeState &state) {
  return new SplitDone(table, old, state);
}

MetaLogEntry *
new_rs_range_loaded(const TableIdentifier &table, const RangeSpec &range,
                    const RangeState &state) {
  return new RangeLoaded(table, range, state);
}

MetaLogEntry *
new_rs_relinquish_start(const TableIdentifier &table, const RangeSpec &old,
                        const RangeState &state) {
  return new RelinquishStart(table, old, state);
}

MetaLogEntry *
new_rs_relinquish_done(const TableIdentifier &table, const RangeSpec &old) {
  return new RelinquishDone(table, old);
}

MetaLogEntry *
new_rs_drop_table(const TableIdentifier &table) {
  return new DropTable(table);
}


MetaLogEntry *
new_from_payload(RangeServerMetaLogEntryType t, int64_t timestamp,
                 StaticBuffer &buf) {
  MetaLogEntry *p = 0;

  switch (t) {
    case RS_SPLIT_START:         p = new SplitStart();        break;
    case RS_SPLIT_SHRUNK:        p = new SplitShrunk();       break;
    case RS_SPLIT_DONE:          p = new SplitDone();         break;
    case RS_RANGE_LOADED:        p = new RangeLoaded();       break;
    case RS_RELINQUISH_START:    p = new RelinquishStart();   break;
    case RS_RELINQUISH_DONE:     p = new RelinquishDone();    break;
    case RS_DROP_TABLE:          p = new DropTable();         break;
    case RS_LOG_RECOVER:         p = new RsmlRecover();       break;
    default: HT_THROWF(Error::METALOG_ENTRY_BAD_TYPE, "unknown type (%d)", t);
  }
  p->timestamp = timestamp;
  p->read(buf);
  return p;
}

}}} // namespace Hypertable::MetaLogEntryFactory
