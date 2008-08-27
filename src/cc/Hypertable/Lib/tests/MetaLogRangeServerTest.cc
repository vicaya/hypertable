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
using namespace MetaLogEntryFactory;

namespace {

const int DFS_TIMEOUT = 15; 
const int DFSBROKER_PORT = 38030;

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
  st.soft_limit = 6400000;
  st.transfer_log = "/test/split.log";
  metalog->log_split_start(table, range, split_off, st);
  st.old_start_row = "0";
  metalog->log_split_shrunk(table, range, st);

  RangeSpec r3("Z", "z");
  RangeState s3;
  s3.state = RangeState::STEADY;
  s3.soft_limit = 6400000;
  metalog->log_range_loaded(table, r3, s3);
}

void
read_test(Filesystem *fs, const String &fname) {
  RangeServerMetaLogReaderPtr reader = new RangeServerMetaLogReader(fs, fname);
  MetaLogEntryPtr log_entry = reader->read();
  const RangeStates &rstates = reader->load_range_states();
  std::ofstream fout("rsmltest.out");
  
  foreach(const RangeStateInfo *i, rstates) fout << *i;
}

} // local namespace

int
main(int ac, char *av[]) {
  try {
    Config::Desc desc("Usage: metalog_rs_test [options]\nOptions");
    desc.add_options()
      ("save", "Don't delete generated the log files")
      ;
    Config::init_with_comm(ac, av, &desc);

    DfsBroker::Client *client = new DfsBroker::Client("localhost",
        DFSBROKER_PORT, DFS_TIMEOUT);

    if (!client->wait_for_connection(DFS_TIMEOUT)) {
      HT_ERROR("Unable to connect to DFS");
      return 1;
    }
    String testdir = format("/rsmltest%d", getpid());
    String logfile = format("%s/rs.log", testdir.c_str());

    client->mkdirs(testdir);

    write_test(client, logfile);
    read_test(client, logfile);

    if (!Config::varmap.count("save"))
      client->rmdir(testdir);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
