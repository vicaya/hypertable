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
  RangeSpec range("0", "Z");
  RangeState state;
  metalog->log_range_loaded(table, range, state);

  RangeSpec split_off("0", "H");
  RangeState st;
  st.state = RangeState::SPLIT_LOG_INSTALLED;
  st.soft_limit = 6400*K;
  st.transfer_log = "/test/split.log";
  metalog->log_split_start(table, range, split_off, st);
  st.old_boundary_row = "0";
  metalog->log_split_shrunk(table, range, st);

  RangeSpec r3("Z", "z");
  RangeState s3;
  s3.state = RangeState::STEADY;
  s3.soft_limit = 6400*K;
  metalog->log_range_loaded(table, r3, s3);
}

void
read_states(Filesystem *fs, const String &fname, const char *outfname) {
  RangeServerMetaLogReaderPtr reader = new RangeServerMetaLogReader(fs, fname);
  MetaLogEntryPtr log_entry = reader->read();
  const RangeStates &rstates = reader->load_range_states();
  std::ofstream fout(outfname);

  foreach(const RangeStateInfo *i, rstates)
    fout << *i;
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
  ml->log_split_start(table, RangeSpec("a", "m"), RangeSpec("a", "g"), s);
}

void
restart_test(Filesystem *fs, const String &fname) {
  write_more(fs, fname);
  read_states(fs, fname, "rsmltest2.out");
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
    String testdir = format("/rsmltest%d", getpid());
    String logfile = format("%s/rs.log", testdir.c_str());

    client->mkdirs(testdir);

    write_test(client, logfile);
    read_states(client, logfile, "rsmltest.out");

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
