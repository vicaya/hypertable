/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include <cassert>
#include <cstdlib>

#include "AsyncComm/Comm.h"

#include "Common/Properties.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/String.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/Defaults.h"
#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/CommitLogReader.h"

#include "DfsBroker/Lib/Client.h"


using namespace Hypertable;

namespace {
  const char *usage[] = {
    "usage: commit_log_test",
    "",
    "Tests the commit log.",
    "",
    0
  };

  void test1(DfsBroker::Client *dfs_client);
  void test_link(DfsBroker::Client *dfs_client);
  void write_entries(CommitLog *log, int num_entries, uint64_t *sump,
                     CommitLogBase *link_log);
  void read_entries(DfsBroker::Client *dfs_client, CommitLogReader *log_reader,
                    uint64_t *sump);
}


int main(int argc, char **argv) {
  ConnectionManagerPtr conn_manager_ptr;
  DfsBroker::Client *dfs_client;

  if (argc == 2 && !strcmp(argv[1], "--help"))
    Usage::dump_and_exit(usage);

  try {

    System::initialize(System::locate_install_dir(argv[0]));
    ReactorFactory::initialize(System::get_processor_count());

    conn_manager_ptr = new ConnectionManager();

    /**
     * connect to DFS broker
     */
    {
      struct sockaddr_in addr;
      InetAddr::initialize(&addr, "localhost",
                           HYPERTABLE_RANGESERVER_COMMITLOG_DFSBROKER_PORT);
      dfs_client = new DfsBroker::Client(conn_manager_ptr, addr, 60);
      if (!dfs_client->wait_for_connection(10)) {
        HT_ERROR("Unable to connect to DFS Broker, exiting...");
        exit(1);
      }
    }

    srandom(1);

    //test1(dfs_client);

    test_link(dfs_client);

  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("%s - %s", e.what(), Error::get_text(e.code()));
    ReactorFactory::destroy();
    return 1;
  }

  ReactorFactory::destroy();
  return 0;
}



namespace {

  void test1(DfsBroker::Client *dfs_client) {
    PropertiesPtr props_ptr = new Properties();
    String log_dir = "/hypertable/test_log";
    String fname;
    CommitLog *log;
    CommitLogReader *log_reader;
    uint64_t sum_written = 0;
    uint64_t sum_read = 0;

    // Remove /hypertable/test_log
    dfs_client->rmdir(log_dir);

    fname = log_dir + "/c";

    // Create /hypertable/test_log/c
    dfs_client->mkdirs(fname);

    props_ptr->set("Hypertable.RangeServer.CommitLog.RollLimit", "2000");

    log = new CommitLog(dfs_client, fname, props_ptr);

    write_entries(log, 20, &sum_written, 0);

    log->close();

    delete log;

    log_reader = new CommitLogReader(dfs_client, fname);

    read_entries(dfs_client, log_reader, &sum_read);

    delete log_reader;

    HT_ASSERT(sum_read == sum_written);
  }

  void test_link(DfsBroker::Client *dfs_client) {
    PropertiesPtr props_ptr = new Properties();
    String log_dir = "/hypertable/test_log";
    String fname;
    CommitLog *log;
    CommitLogReaderPtr log_reader_ptr;
    uint64_t sum_written = 0;
    uint64_t sum_read = 0;

    // Remove /hypertable/test_log
    dfs_client->rmdir(log_dir);

    // Create log directories
    dfs_client->mkdirs(log_dir + "/a");
    dfs_client->mkdirs(log_dir + "/b");
    dfs_client->mkdirs(log_dir + "/c");
    dfs_client->mkdirs(log_dir + "/d");

    props_ptr->set("Hypertable.RangeServer.CommitLog.RollLimit", "2000");

    /**
     * Create log "c"
     */
    fname = log_dir + "/c";
    log = new CommitLog(dfs_client, fname, props_ptr);
    write_entries(log, 20, &sum_written, 0);
    delete log;

    log_reader_ptr = new CommitLogReader(dfs_client, fname);
    read_entries(dfs_client, log_reader_ptr.get(), &sum_read);

    /**
     * Create log "b" and link in log "c"
     */
    fname = log_dir + "/b";
    log = new CommitLog(dfs_client, fname, props_ptr);
    write_entries(log, 20, &sum_written, log_reader_ptr.get());
    delete log;

    /**
     * Create log "d"
     */
    fname = log_dir + "/d";
    log = new CommitLog(dfs_client, fname, props_ptr);
    write_entries(log, 20, &sum_written, 0);
    delete log;

    /**
     * Create log "a" and link in "b" and "d"
     */
    fname = log_dir + "/a";
    log = new CommitLog(dfs_client, fname, props_ptr);

    // Open "b", read it, and link it into "a"
    fname = log_dir + "/b";
    log_reader_ptr = new CommitLogReader(dfs_client, fname);
    read_entries(dfs_client, log_reader_ptr.get(), &sum_read);
    write_entries(log, 20, &sum_written, log_reader_ptr.get());

    // Open "d", read it, and link it into "a"
    fname = log_dir + "/d";
    log_reader_ptr = new CommitLogReader(dfs_client, fname);
    read_entries(dfs_client, log_reader_ptr.get(), &sum_read);
    write_entries(log, 20, &sum_written, log_reader_ptr.get());

    delete log;

    sum_read = 0;
    fname = log_dir + "/a";
    log_reader_ptr = new CommitLogReader(dfs_client, fname);
    read_entries(dfs_client, log_reader_ptr.get(), &sum_read);

    HT_ASSERT(sum_read == sum_written);
  }

  void
  write_entries(CommitLog *log, int num_entries, uint64_t *sump,
                CommitLogBase *link_log) {
    int error;
    int64_t revision;
    uint32_t limit;
    uint32_t payload[101];
    size_t link_point = 50;
    DynamicBuffer dbuf;

    if (link_log)
      link_point = random() % 50;

    for (size_t i=0; i<50; i++) {
      revision = log->get_timestamp();

      if (i == link_point) {
        if ((error = log->link_log(link_log)) != Error::OK)
          HT_THROW(error, "Problem writing to log file");
      }
      else {
        limit = (random() % 100) + 1;
        for (size_t j=0; j<limit; j++) {
          payload[j] = random();
          *sump += payload[j];
        }

        dbuf.base = (uint8_t *)payload;
        dbuf.ptr = dbuf.base + (4*limit);
        dbuf.own = false;

        if ((error = log->write(dbuf, revision)) != Error::OK)
          HT_THROW(error, "Problem writing to log file");
      }
    }
  }

  void
  read_entries(DfsBroker::Client *dfs_client, CommitLogReader *log_reader,
               uint64_t *sump) {
    const uint8_t *block;
    size_t block_len;
    uint32_t *iptr;
    size_t icount;
    BlockCompressionHeaderCommitLog header;

    while (log_reader->next(&block, &block_len, &header)) {
      assert((block_len % 4) == 0);
      icount = block_len / 4;
      iptr = (uint32_t *)block;
      for (size_t i=0; i<icount; i++)
        *sump += iptr[i];
    }
  }

}
