/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
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
#include <cstdlib>
#include <iostream>

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/Init.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/ReactorFactory.h"

#include "DfsBroker/Lib/Config.h"
#include "DfsBroker/Lib/Client.h"

#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/CommitLogReader.h"

using namespace Hypertable;
using namespace Config;
using namespace std;

namespace {

struct AppPolicy : Config::Policy {
  static void init_options() {
    cmdline_desc("Usage: %s [options] <log-dir>\n\n"
      "  This program dumps the given log's metadata.\n\nOptions")
      .add_options()
      ("block-summary", "Display commit log block information only")
      ("display-values", "Display values (assumes they're printable)")
      ;
    cmdline_hidden_desc().add_options()("log-dir", str(), "dfs log dir");
    cmdline_positional_desc().add("log-dir", -1);
  }
  static void init() {
    if (!has("log-dir")) {
      HT_ERROR_OUT <<"log-dir required\n"<< cmdline_desc() << HT_END;
      exit(1);
    }
  }
};

typedef Meta::list<AppPolicy, DfsClientPolicy, DefaultCommPolicy> Policies;

void display_log(DfsBroker::Client *dfs_client, const String &prefix,
    CommitLogReader *log_reader, bool display_values);
void display_log_block_summary(DfsBroker::Client *dfs_client,
    const String &prefix, CommitLogReader *log_reader);

} // local namespace


int main(int argc, char **argv) {
  try {
    init_with_policies<Policies>(argc, argv);

    ConnectionManagerPtr conn_manager_ptr = new ConnectionManager();

    String log_dir = get_str("log-dir");
    String log_host = get("log-host", String());
    int timeout = get_i32("dfs-timeout");
    bool block_summary = has("block-summary");

    /**
     * Check for and connect to commit log DFS broker
     */
    DfsBroker::Client *dfs_client;

    if (log_host.length()) {
      int log_port = get_i16("log-port");
      InetAddr addr(log_host, log_port);

      dfs_client = new DfsBroker::Client(conn_manager_ptr, addr, timeout);
    }
    else {
      dfs_client = new DfsBroker::Client(conn_manager_ptr, properties);
    }

    if (!dfs_client->wait_for_connection(timeout)) {
      HT_ERROR("Unable to connect to DFS Broker, exiting...");
      exit(1);
    }

    FilesystemPtr fs = dfs_client;

    CommitLogReaderPtr log_reader = new CommitLogReader(fs, log_dir);

    if (block_summary) {
      printf("LOG %s\n", log_dir.c_str());
      display_log_block_summary(dfs_client, "", log_reader.get());
    }
    else
      display_log(dfs_client, "", log_reader.get(), has("display_values"));
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}


namespace {

  void
  display_log(DfsBroker::Client *dfs_client, const String &prefix,
              CommitLogReader *log_reader, bool display_values) {
    BlockCompressionHeaderCommitLog header;
    const uint8_t *base;
    size_t len;
    const uint8_t *ptr, *end;
    TableIdentifier table_id;
    ByteString bs;
    Key key;
    String value;
    uint32_t blockno=0;

    while (log_reader->next(&base, &len, &header)) {

      HT_ASSERT(header.check_magic(CommitLog::MAGIC_DATA));

      ptr = base;
      end = base + len;

      table_id.decode(&ptr, &len);

      while (ptr < end) {

        // extract the key
        bs.ptr = ptr;
        key.load(bs);
        cout << key;
        bs.next();

        if (display_values) {
          const uint8_t *vptr;
          size_t slen = bs.decode_length(&vptr);
          cout << " value='" << std::string((char *)vptr, slen) << "'";
        }

        //cout << " bno=" << blockno << endl;
        cout << endl;

        // skip value
        bs.next();
        ptr = bs.ptr;

        if (ptr > end)
          HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding value");
      }
      blockno++;
    }
  }



  void
  display_log_block_summary(DfsBroker::Client *dfs_client, const String &prefix,
      CommitLogReader *log_reader) {
    CommitLogBlockInfo binfo;
    BlockCompressionHeaderCommitLog header;

    while (log_reader->next_raw_block(&binfo, &header)) {

      HT_ASSERT(header.check_magic(CommitLog::MAGIC_DATA));

      printf("%s%s\trevision\t%llu\n",
             binfo.log_dir, binfo.file_fragment, (Llu)header.get_revision());
      printf("%s%s\tstart-offset\t%llu\n",
             binfo.log_dir, binfo.file_fragment, (Llu)binfo.start_offset);
      printf("%s%s\tend-offset\t%llu\n",
             binfo.log_dir, binfo.file_fragment, (Llu)binfo.end_offset);

      if (binfo.error == Error::OK) {
        printf("%s%s\tlength\t%u\n",
               binfo.log_dir, binfo.file_fragment,
               header.get_data_length());
        printf("%s%s\tztype\t%s\n",
               binfo.log_dir, binfo.file_fragment,
               BlockCompressionCodec::get_compressor_name(header.get_compression_type()));
        printf("%s%s\tzlen\t%u\n",
               binfo.log_dir, binfo.file_fragment,
               header.get_data_zlength());
      }
      else
        printf("%s%s\terror\t%s\n",
               binfo.log_dir, binfo.file_fragment,
               Error::get_text(binfo.error));
    }
  }

} // local namespace
