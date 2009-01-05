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
#include "Common/InetAddr.h"
#include <iostream>
#include <fstream>
#include "DfsBroker/Lib/Client.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ReactorFactory.h"
#include "AsyncComm/ConnectionManager.h"
#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/RangeServerMetaLogEntryFactory.h"
#include "Hypertable/Lib/RangeServerMetaLogReader.h"
#include "Hypertable/Lib/RangeServerMetaLog.h"

using namespace Hypertable;
using namespace Config;
using namespace MetaLogEntryFactory;

namespace {

struct MyPolicy : Policy {
  static void init_options() {
    cmdline_desc().add_options()
      ("save,s", "Don't delete generated the log files")
      ("no-diff,n", "Don't compare with golden files yet")
      ("no-restart", "Skip restart tests")
      ;
  }
};

typedef Meta::list<MyPolicy, DfsClientPolicy, DefaultCommPolicy> Policies;

void
check_diff(const char *out, const char *golden) {
  HT_EXPECT(std::system(format("diff %s %s", out, golden).c_str()) == 0,
            Error::EXTERNAL);
}

void
write_test(Filesystem *fs, const String &fname) {
  RangeServerMetaLogPtr metalog = new RangeServerMetaLog(fs, fname);

  TableIdentifier table("rsmltest");

  // 1. split off = low
  RangeSpec r1("0", "Z");
  RangeState st;
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
  metalog->log_range_loaded(table, r2high, RangeState());

  metalog->log_split_done(table, r2, st);
  st.clear();

  // 3. r2 split off again = high, without finish
  st.state = RangeState::SPLIT_LOG_INSTALLED;
  st.transfer_log = "/test/split3.log";
  st.split_point = "A";
  st.old_boundary_row = "a"; // split off high
  metalog->log_split_start(table, r2, st);
}

void
read_states(Filesystem *fs, const String &fname, std::ostream &out) {
  RangeServerMetaLogReaderPtr reader = new RangeServerMetaLogReader(fs, fname);

  out <<"Log entries:\n";

  MetaLogEntryPtr entry;

  while ((entry = reader->read()))
    out << entry.get() <<"\n";

  out <<"Range states:\n";

  const RangeStates &rstates = reader->load_range_states();

  foreach(const RangeStateInfo *i, rstates)
    out << *i;
}

void
write_more(Filesystem *fs, const String &fname) {
  RangeServerMetaLogPtr ml = new RangeServerMetaLog(fs, fname);

  TableIdentifier table("rsmltest");
  RangeState s;

  s.soft_limit = 40*M;
  ml->log_range_loaded(table, RangeSpec("m", "s"), s);

  s.state = RangeState::SPLIT_LOG_INSTALLED;
  s.transfer_log = "/ha/split.log";
  ml->log_split_start(table, RangeSpec("a", "m"), s);
}

void
restart_test(Filesystem *fs, const String &fname) {
  write_more(fs, fname);
  std::ofstream out("rsmltest2.out");
  read_states(fs, fname, out);
}

} // local namespace

int
main(int ac, char *av[]) {
  try {
    init_with_policies<Policies>(ac, av);

    bool nodiff = has("no-diff") || 1;          // TODO
    bool norestart = has("no-restart") || 1;    // TODO
    int timeout = get_i32("dfs-timeout");
    String host = get_str("dfs-host");
    uint16_t port = get_i16("dfs-port");

    DfsBroker::Client *client = new DfsBroker::Client(host, port, timeout);

    if (!client->wait_for_connection(timeout)) {
      HT_ERROR_OUT <<"Unable to connect to DFS: "<< host <<':'<< port << HT_END;
      return 1;
    }
    std::ofstream out("rsmltest.out");

    String testdir = format("/rsmltest%d", getpid());
    String logfile = format("%s/rs.log", testdir.c_str());
    client->mkdirs(testdir);

    out <<"logfile="<< logfile <<'\n';
    write_test(client, logfile);
    read_states(client, logfile, out);

    if (!nodiff)
      check_diff("rsmltest.out", "rsmltest.golden");

    if (!norestart)
      restart_test(client, logfile);

    if (!nodiff)
      check_diff("rsmltest2.out", "rsmltest2.golden");

    if (!has("save"))
      client->rmdir(testdir);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
