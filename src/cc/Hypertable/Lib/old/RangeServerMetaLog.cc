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
#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"
#include "Common/Filesystem.h"
#include "RangeServerMetaLogReader.h"
#include "RangeServerMetaLog.h"
#include "MetaLogVersion.h"
#include "RangeServerMetaLogEntryFactory.h"
#include "RangeServerMetaLogEntries.h"

using namespace Hypertable;
using namespace Hypertable::OldMetaLog;
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
  : Parent(fs, path, "rsml_backup") {

  while (fd() == -1) {
    HT_DEBUG_OUT << path <<" exists, recovering..."<< HT_END;
    if (!recover(path)) {
      find_or_create_file();
      continue;
    }
    return;
  }

  write_header();

  // Write RS_LOG_RECOVER entry
  MetaLogEntryPtr entry = new RangeServerTxn::RsmlRecover();
  write(entry.get());

}

void RangeServerMetaLog::write_header() {
  StaticBuffer buf(RSML_HEADER_SIZE);
  MetaLogHeader header(RSML_PREFIX, RSML_VERSION);
  header.encode(buf.base, RSML_HEADER_SIZE);

  FileUtils::write(backup_fd(), buf.base, RSML_HEADER_SIZE);

  if (fs().append(fd(), buf, 1) != RSML_HEADER_SIZE)
    HT_THROWF(Error::DFSBROKER_IO_ERROR, "Error writing range server "
              "metalog header to file: %s", path().c_str());

  HT_DEBUG_OUT << header << HT_END;
}

bool
RangeServerMetaLog::recover(const String &path) {
  bool found_recover_entry = false;

  RangeServerMetaLogReaderPtr reader =
    new RangeServerMetaLogReader(&fs(), path);

  {
    DynamicBuffer buf( (fs().length(filename()) * 2) + 32 );
    MetaLogHeader header(RSML_PREFIX, RSML_VERSION);

    // write header
    header.encode(buf.base, RSML_HEADER_SIZE);
    buf.ptr += RSML_HEADER_SIZE;

    // copy entries
    MetaLogEntryPtr entry;
    try {
      MetaLogEntries entries;
      RangeStates range_states = reader->load_range_states(&found_recover_entry);

      foreach(const RangeStateInfo *state, range_states)
        foreach (const MetaLogEntryPtr &e, state->transactions)
          entries.push_back(e);
      std::sort(entries.begin(), entries.end(), OrderByTimestamp());
      foreach(MetaLogEntryPtr &e, entries)
        serialize_entry(e.get(), buf);
    }
    catch (Hypertable::Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      if (e.code() == Error::DFSBROKER_EOF)
        HT_ERRORF("Truncated RSML file '%s', system may be corrupt.",
                  filename().c_str());
    }

    if (found_recover_entry == false) {
      HT_WARNF("RS_LOG_RECOVER entry not found in RSML '%s', removing file...",
               filename().c_str());
      reader = 0; // close any open file handles
      fs().remove(filename());
      return false;
    }

    // write RS_LOG_RECOVER entry
    entry = new RangeServerTxn::RsmlRecover();
    serialize_entry(entry.get(), buf);

    // create next log file, incremented by 1
    String tmp = path;
    tmp += String("/") + (fileno()+1);
    fd(create(tmp, true));
    filename(tmp);

    // create next backup log file, incremented by 1
    tmp = backup_path();
    tmp += String("/") + (fileno()+1);
    backup_fd(create_backup(tmp));
    backup_filename(tmp);

    write_unlocked(buf);
  }

  return true;

}

void
RangeServerMetaLog::purge(const RangeStates &rs) {
  // write purged entries in a tmp file
  String tmp = format("%s%d", path().c_str(), (int)getpid());
  int fd = create(tmp, true);
  MetaLogEntries entries;

  foreach(const RangeStateInfo *i, rs) {
    foreach (const MetaLogEntryPtr &p, i->transactions)
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
