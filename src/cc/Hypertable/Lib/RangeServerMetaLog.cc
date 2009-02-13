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
#include "Common/Serialization.h"
#include "Common/Logger.h"
#include "Filesystem.h"
#include "RangeServerMetaLogReader.h"
#include "RangeServerMetaLog.h"
#include "MetaLogVersion.h"
#include "RangeServerMetaLogEntryFactory.h"

using namespace Hypertable;
using namespace Serialization;
using namespace MetaLogEntryFactory;

namespace {

struct OrderByTimestamp {
  bool operator()(const MetaLogEntryPtr &x, const MetaLogEntryPtr &y)  {
    return x->timestamp < y->timestamp;
  }
};

} // local namespace

RangeServerMetaLog::RangeServerMetaLog(Filesystem *fs, const String &path)
    : Parent(fs, path) {

  if (fd() == -1) {
    HT_DEBUG_OUT << path <<" exists, recovering..."<< HT_END;
    recover(path);
    return;
  }
  write_header();
}

void RangeServerMetaLog::write_header() {
  StaticBuffer buf(RSML_HEADER_SIZE);
  MetaLogHeader header(RSML_PREFIX, RSML_VERSION);
  header.encode(buf.base, RSML_HEADER_SIZE);

  if (fs().append(fd(), buf, 1) != RSML_HEADER_SIZE)
    HT_THROWF(Error::DFSBROKER_IO_ERROR, "Error writing range server "
              "metalog header to file: %s", path().c_str());

  HT_DEBUG_OUT << header << HT_END;
}

void
RangeServerMetaLog::recover(const String &path) {
  String tmp(path);
  tmp += ".tmp";

  fd(create(tmp, true));
  write_header();

  // copy the metalog and potentially skip the last bad entry
  RangeServerMetaLogReaderPtr reader =
      new RangeServerMetaLogReader(&fs(), path);

  MetaLogEntryPtr entry;

  while ((entry = reader->read()))
    write(entry.get());

  fs().rename(tmp, path);
}

void
RangeServerMetaLog::purge(const RangeStates &rs) {
  // write purged entries in a tmp file
  String tmp = format("%s%d", path().c_str(), getpid());
  int fd = create(tmp, true);
  MetaLogEntries entries;

  foreach(const RangeStateInfo *i, rs) {
    if (i->transactions.empty()) {
      MetaLogEntry *entry = new_rs_range_loaded(i->table, i->range,
                                                i->range_state);
      entry->timestamp = i->timestamp; // important
      entries.push_back(entry);
    }
    else foreach (const MetaLogEntryPtr &p, i->transactions)
      entries.push_back(p);
  }
  std::sort(entries.begin(), entries.end(), OrderByTimestamp());
  foreach(MetaLogEntryPtr &e, entries) write(e.get());
  fs().close(fd);

  // rename existing log to name.save and tmp file to the log name
  {
    ScopedLock lock(mutex());
    String save = format("%s.save", path().c_str());
    fs().rename(path(), save);
    fs().rename(tmp, path());
  }

  // reopen
  Parent::fd(create(path()));
}
