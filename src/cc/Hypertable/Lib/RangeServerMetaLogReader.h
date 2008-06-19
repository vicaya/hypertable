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

#ifndef HYPERTABLE_RANGE_SERVER_METALOG_READER_H
#define HYPERTABLE_RANGE_SERVER_METALOG_READER_H

#include "Types.h"
#include "RangeState.h"
#include "MetaLogReaderDfsBase.h"

namespace Hypertable {

struct RangeStateInfo {
  RangeStateInfo(const TableIdentifier &tid, const RangeSpec &r, 
                 uint64_t slimit, uint64_t ts)
      : table(tid), range(r), soft_limit(slimit), timestamp(ts) {}

  RangeStateInfo(const TableIdentifier &tid, const RangeSpec &r)
      : table(TableIdentifier()), range(RangeSpec()) {
    // constructing lookup key only no copy needed
    table.id = tid.id;
    range.end_row = r.end_row;
  }

  TableIdentifierManaged table;
  RangeSpecManaged range;
  uint64_t soft_limit;
  uint64_t timestamp; // time when the range is loaded
  MetaLogEntries transactions; // log entries associated with current txn
};

std::ostream &operator<<(std::ostream &, const RangeStateInfo &);

typedef std::vector<RangeStateInfo *> RangeStates;

class RangeServerMetaLogReader : public MetaLogReaderDfsBase {
public:
  typedef MetaLogReaderDfsBase Parent;

  RangeServerMetaLogReader(Filesystem *, const String& path);
  virtual ~RangeServerMetaLogReader();

  virtual MetaLogEntry *read();

  const RangeStates &load_range_states(bool force = false);

private:
  RangeStates m_range_states;
};

typedef intrusive_ptr<RangeServerMetaLogReader> RangeServerMetaLogReaderPtr;

} // namespace Hypertable

#endif // HYPERTABLE_RANGE_SERVER_METALOG_READER_H
