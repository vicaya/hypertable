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

#include "Common/String.h"
#include "Types.h"
#include "RangeServerMetaLog.h"
#include "MetaLogReader.h"

namespace Hypertable {

struct RangeStateInfo {
  RangeSpec range;
  MetaLogEntries transactions; // log entries associated with current txn
};

typedef std::vector<RangeStateInfo> RangeStates;

class Filesystem;

class RangeServerMetaLogReader : public MetaLogReader {
public:
  RangeServerMetaLogReader(Filesystem *, const String& path);

  virtual ScanEntry *next(ScanEntry *);
  virtual RangeServerMetaLogEntry *read();

  void load_range_states(RangeStates &);
};

} // namespace Hypertable

#endif // HYPERTABLE_RANGE_SERVER_METALOG_READER_H
