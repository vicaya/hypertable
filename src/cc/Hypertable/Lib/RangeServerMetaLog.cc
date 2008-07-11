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
#include <unistd.h>
#include "Common/Serialization.h"
#include "Filesystem.h"
#include "RangeServerMetaLog.h"
#include "MetaLogVersion.h"
#include "RangeServerMetaLogEntryFactory.h"

using namespace std;
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
  if (fs->length(path))
    return;
  uint8_t buf[RSML_HEADER_SIZE], *p = buf;
  memcpy(buf, RSML_PREFIX, strlen(RSML_PREFIX));
  p += strlen(RSML_PREFIX);
  encode_i16(&p, RSML_VERSION);

  StaticBuffer sbuf(buf, RSML_HEADER_SIZE, false);

  if (fs->append(fd(), sbuf, 0) != RSML_HEADER_SIZE)
    HT_THROWF(Error::DFSBROKER_IO_ERROR, "Error writing range server "
              "metalog header to file: %s", path.c_str());
}

void
RangeServerMetaLog::purge(const RangeStates &rs) {
  // write purged entries in a tmp file
  String tmp = format("%s%d", path().c_str(), getpid());
  int fd = create(tmp, true);
  MetaLogEntries entries;

  foreach(const RangeStateInfo *i, rs) {
    if (i->transactions.empty()) {
      RangeState state(RangeState::STEADY, i->soft_limit, 0);
      entries.push_back(new_rs_range_loaded(i->table, i->range, state));
    }
    else foreach (const MetaLogEntryPtr &p, i->transactions)
      entries.push_back(p);
  }
  sort(entries.begin(), entries.end(), OrderByTimestamp());
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
