/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
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
#include "Hypertable/Lib/old/MasterMetaLogEntryFactory.h"
#include "Hypertable/Lib/old/MasterMetaLogReader.h"
#include "Hypertable/Lib/old/MasterMetaLog.h"

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
write_test(Filesystem *fs, const String &fname) {
  MasterMetaLogPtr metalog = new MasterMetaLog(fs, fname);

  TableIdentifier table("1");

  metalog->log_server_joined("rs8");
  metalog->log_server_joined("rs1");
  metalog->log_server_joined("rs7");
  metalog->log_server_joined("rs3");
  metalog->log_server_joined("rs2");
  metalog->log_server_joined("rs6");
  metalog->log_server_joined("rs4");
  metalog->log_server_joined("rs5");

  metalog->log_balance_started();
  RangeSpec r1("A", "B");
  metalog->log_range_move_started(table, r1, "/logs/1", 1234, "rs3");
  metalog->log_server_left("rs3");

  RangeSpec r2("B", "C");
  metalog->log_range_move_started(table, r2, "/logs/2", 2345, "rs6");

  RangeSpec r3("C", "D");
  metalog->log_range_move_started(table, r3, "/logs/3", 3456, "rs7");

  metalog->log_server_left("rs1");

  metalog->log_range_move_loaded(table, r2, "rs6");
  metalog->log_range_move_acknowledged(table, r2, "rs6");

  metalog->log_server_left("rs5");
  metalog->log_server_removed("rs5");

  metalog->log_server_joined("rs11");
  RangeSpec r4("E", "F");
  metalog->log_range_move_started(table, r4, "/logs/4", 4567, "rs11");
  metalog->log_balance_done();
  metalog->log_balance_started();
}

void
read_states(Filesystem *fs, const String &fname, std::ostream &out) {
  MasterMetaLogReaderPtr reader = new MasterMetaLogReader(fs, fname);

  out <<"Log entries:\n";

  MetaLogEntryPtr entry;

  while ((entry = reader->read()))
    out << entry.get() <<"\n";

  out <<"Range states:\n";

  bool found_recover_entry;
  const ServerStates &sstates = reader->load_server_states(&found_recover_entry);
  if (found_recover_entry)
    out << "Found recover entry\n";
  else
    out << "Recover entry not found\n";

  foreach(ServerStateInfo *i, sstates) {
    i->timestamp = 0;
    out << *i;
  }
}

void
write_more(Filesystem *fs, const String &fname) {
  MasterMetaLogPtr metalog = new MasterMetaLog(fs, fname);

  TableIdentifier table("1");

  metalog->log_server_joined("rs9");
  metalog->log_server_joined("rs12");
  metalog->log_server_joined("rs10");

  RangeSpec r1("E", "F");
  metalog->log_server_left("rs7");

  RangeSpec r2("F", "G");
  metalog->log_range_move_started(table, r2, "/logs/5", 5678, "rs4");

  RangeSpec r3("G", "H");
  metalog->log_range_move_started(table, r3, "/logs/6", 6789, "rs2");
  metalog->log_range_move_loaded(table, r3, "rs2");

  metalog->log_range_move_acknowledged(table, r1, "rs11");

  metalog->log_server_left("rs10");

  metalog->log_range_move_loaded(table, r1, "rs11");

}

void
restart_test(Filesystem *fs, const String &fname) {
  write_more(fs, fname);
  std::ofstream out("mmltest2.out");
  read_states(fs, fname, out);
}

void
write_more_again(Filesystem *fs, const String &fname) {
  MasterMetaLogPtr metalog = new MasterMetaLog(fs, fname);

  TableIdentifier table("1");

  metalog->log_balance_done();
  metalog->log_server_joined("rs15");
  metalog->log_server_joined("rs14");
  metalog->log_server_joined("rs16");
  metalog->log_server_joined("rs13");

  RangeSpec r1("H", "I");
  metalog->log_range_move_started(table, r1, "/logs/7", 7891, "rs16");
  metalog->log_server_left("rs11");

  RangeSpec r2("I", "J");
  metalog->log_range_move_started(table, r2, "/logs/8", 8912, "rs13");

  RangeSpec r3("J", "K");
  metalog->log_range_move_started(table, r3, "/logs/9", 9123, "rs1");
  metalog->log_range_move_loaded(table, r2, "rs13");
  metalog->log_range_move_acknowledged(table, r2, "rs13");

  metalog->log_server_removed("rs11");

  RangeSpec r4("G", "H");
  metalog->log_range_move_acknowledged(table, r4, "rs2");
}

void
restart_test_again(Filesystem *fs, const String &fname) {
  write_more_again(fs, fname);
  std::ofstream out("mmltest3.out");
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
      std::ofstream out("mmltest.out");

      testdir = format("/mmltest%09d", (int)getpid());
      client->mkdirs(testdir);

      out <<"testdir="<< testdir <<'\n';
      write_test(client, testdir);
      read_states(client, testdir, out);
      out << std::flush;
    }

    HT_ASSERT(FileUtils::size("mmltest.out") == FileUtils::size("mmltest.golden"));

    restart_test(client, testdir);

    HT_ASSERT(FileUtils::size("mmltest2.out") == FileUtils::size("mmltest2.golden"));

    restart_test_again(client, testdir);

    HT_ASSERT(FileUtils::size("mmltest3.out") == FileUtils::size("mmltest3.golden"));

    // Now created a truncated MML file '3'
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

    // Now read the MML and
    {
      MasterMetaLogPtr ml = new MasterMetaLog(client, testdir);

      {
        std::ofstream out("mmltest4.out");
        read_states(client, testdir, out);
      }

      HT_ASSERT(FileUtils::size("mmltest4.out") == FileUtils::size("mmltest4.golden"));
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
