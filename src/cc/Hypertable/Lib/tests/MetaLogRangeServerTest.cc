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
#include "Common/FileUtils.h"
#include "Common/Init.h"
#include "Common/InetAddr.h"
#include "Common/StringExt.h"
#include <iostream>
#include <fstream>
#include "DfsBroker/Lib/Client.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ReactorFactory.h"
#include "AsyncComm/ConnectionManager.h"
#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/old/RangeServerMetaLogEntryFactory.h"
#include "Hypertable/Lib/old/RangeServerMetaLogReader.h"
#include "Hypertable/Lib/old/RangeServerMetaLog.h"

using namespace Hypertable;
using namespace Config;
using namespace Hypertable::OldMetaLog;
using namespace Hypertable::OldMetaLog::MetaLogEntryFactory;

namespace {

struct MyPolicy : Policy {
  static void init_options() {
    cmdline_desc().add_options()
      ("save,s", "Don't delete generated the log files")
      ;
  }
};

typedef Meta::list<MyPolicy, DfsClientPolicy, DefaultCommPolicy> Policies;

void
verify_size(const char *out, const char *golden) {
  HT_ASSERT(FileUtils::size(out) == FileUtils::size(golden));
}

void
write_test(Filesystem *fs, const String &fname) {
  RangeServerMetaLogPtr metalog = new RangeServerMetaLog(fs, fname);

  TableIdentifier table("1");

  // 1. split off = low
  RangeSpec r1("0", "Z");
  RangeState st;

  st.clear();

  metalog->log_range_loaded(table, r1, st);

  st.state = RangeState::SPLIT_LOG_INSTALLED;
  st.transfer_log = "/test/split.log";
  st.split_point = "A";
  st.old_boundary_row = "0"; // split off low
  metalog->log_split_start(table, r1, st);

  st.state = RangeState::SPLIT_SHRUNK;
  r1.start_row = "A";
  metalog->log_split_shrunk(table, r1, st);

  metalog->log_split_done(table, r1, st);
  st.clear();

  // 2. split off = high
  RangeSpec r2("Z", "z");
  metalog->log_range_loaded(table, r2, st);

  st.state = RangeState::SPLIT_LOG_INSTALLED;
  st.transfer_log = "/test/split2.log";
  st.split_point = "a";
  st.old_boundary_row = "z"; // split off high
  metalog->log_split_start(table, r2, st);

  st.state = RangeState::SPLIT_SHRUNK;
  r2.end_row = "a";
  metalog->log_split_shrunk(table, r2, st);

  // the split off range gets loaded here
  RangeSpec r2high("a", "z");
  RangeState st2;
  st2.clear();
  metalog->log_range_loaded(table, r2high, st2);

  metalog->log_split_done(table, r2, st);
  st.clear();

  // 3. r2 split off again = high, without finish
  st.state = RangeState::SPLIT_LOG_INSTALLED;
  st.transfer_log = "/test/split3.log";
  st.split_point = "m";
  st.old_boundary_row = "z"; // split off high
  metalog->log_split_start(table, r2high, st);

  // 4. start range relinquish
  st.clear();
  st.soft_limit = 40*M;
  metalog->log_range_loaded(table, RangeSpec("s", "v"), st);
  metalog->log_relinquish_start(table, RangeSpec("s", "v"), st);

}

void
read_states(Filesystem *fs, const String &fname, std::ostream &out) {
  RangeServerMetaLogReaderPtr reader = new RangeServerMetaLogReader(fs, fname);

  out <<"Log entries:\n";

  MetaLogEntryPtr entry;

  while ((entry = reader->read()))
    out << entry.get() <<"\n";

  out <<"Range states:\n";

  bool found_recover_entry;
  const RangeStates &rstates = reader->load_range_states(&found_recover_entry);
  if (found_recover_entry)
    out << "Found recover entry\n";
  else
    out << "Recover entry not found\n";

  foreach(RangeStateInfo *i, rstates) {
    i->timestamp = 0;
    out << *i;
  }
}

void
write_more(Filesystem *fs, const String &fname) {
  RangeServerMetaLogPtr ml = new RangeServerMetaLog(fs, fname);

  TableIdentifier table("1");
  RangeState s;

  // finish range relinquish
  s.clear();
  s.soft_limit = 40*M;
  ml->log_relinquish_done(table, RangeSpec("s", "v"));

  s.clear();
  s.soft_limit = 40*M;
  ml->log_range_loaded(table, RangeSpec("m", "s"), s);

  // the split off range gets loaded here
  RangeSpec rs("a", "z");
  s.clear();
  s.soft_limit = 40*M;
  s.state = RangeState::SPLIT_SHRUNK;
  s.old_boundary_row = "z"; // split off high
  rs.end_row = "m";
  ml->log_split_shrunk(table, rs, s);


}

void
restart_test(Filesystem *fs, const String &fname) {
  write_more(fs, fname);
  std::ofstream out("rsmltest2.out");
  read_states(fs, fname, out);
}

void
write_more_again(Filesystem *fs, const String &fname) {
  RangeServerMetaLogPtr ml = new RangeServerMetaLog(fs, fname);

  TableIdentifier table("1");
  RangeSpec rs("a", "z");
  RangeState s;

  s.clear();
  s.soft_limit = 40*M;
  s.state = RangeState::SPLIT_SHRUNK;
  s.old_boundary_row = "z"; // split off high
  rs.end_row = "m";

  ml->log_split_done(table, rs, s);

}

void
restart_test_again(Filesystem *fs, const String &fname) {
  write_more_again(fs, fname);
  std::ofstream out("rsmltest3.out");
  read_states(fs, fname, out);
}


} // local namespace

int
main(int ac, char *av[]) {
  try {
    init_with_policies<Policies>(ac, av);

    int timeout = has("dfs-timeout") ? get_i32("dfs-timeout") : 180000;
    String host = get_str("dfs-host");
    uint16_t port = get_i16("dfs-port");

    DfsBroker::Client *client = new DfsBroker::Client(host, port, timeout);

    if (!client->wait_for_connection(timeout)) {
      HT_ERROR_OUT <<"Unable to connect to DFS: "<< host <<':'<< port << HT_END;
      return 1;
    }

    String testdir;

    {
      std::ofstream out("rsmltest.out");

      testdir = format("/rsmltest%09d", (int)getpid());
      client->mkdirs(testdir);

      out <<"testdir="<< testdir <<'\n';
      write_test(client, testdir);
      read_states(client, testdir, out);
      out << std::flush;
    }

    HT_ASSERT(FileUtils::size("rsmltest.out") == FileUtils::size("rsmltest.golden"));

    restart_test(client, testdir);

    HT_ASSERT(FileUtils::size("rsmltest2.out") == FileUtils::size("rsmltest2.golden"));

    restart_test_again(client, testdir);

    HT_ASSERT(FileUtils::size("rsmltest3.out") == FileUtils::size("rsmltest3.golden"));

    // Now created a truncated RSML file '3'
    String source_file = testdir;
    source_file += String("/") + 2;
    int64_t log_size = client->length(source_file) / 2;

    String dest_file = testdir;
    dest_file += String("/") + 3;

    StaticBuffer sbuf(log_size);

    int src_fd = client->open(source_file);
    client->read(src_fd, sbuf.base, log_size);
    client->close(src_fd);

    int dst_fd = client->create(dest_file, Filesystem::OPEN_FLAG_OVERWRITE, -1, -1, -1);
    client->append(dst_fd, sbuf, Filesystem::O_FLUSH);
    client->close(dst_fd);

    // Now read the RSML and
    {
      RangeServerMetaLogPtr ml = new RangeServerMetaLog(client, testdir);

      {
        std::ofstream out("rsmltest4.out");
        read_states(client, testdir, out);
      }

      // size of rsml dump should be same as the last one
      HT_ASSERT(FileUtils::size("rsmltest4.out") == FileUtils::size("rsmltest4.golden"));
    }

    if (!has("save"))
      client->rmdir(testdir);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
