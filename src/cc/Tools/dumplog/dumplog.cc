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

extern "C" {
#include <netdb.h>
}

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/ReactorFactory.h"

#include "DfsBroker/Lib/Client.h"

#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/CommitLogReader.h"

using namespace Hypertable;
using namespace std;

namespace {

  const char *usage[] = {
    "usage: dumplog [options] <log-dir>",
    "",
    "  options:",
    "    --block-summary  Display commit log block information only",
    "    --config=<file>  Read configuration from <file>.  The default config file",
    "                     is \"conf/hypertable.cfg\" relative to the toplevel",
    "                     install directory",
    "    --display-values Display values (assumes they're printable)",
    "    --help           Display this help text and exit",
    "    --verbose,-v     Display 'true' if up, 'false' otherwise",
    "",
    "  This program dumps the given log's metadata.",
    "",
    (const char *)0
  };

  void display_log(DfsBroker::Client *dfs_client, String prefix, CommitLogReader *log_reader, bool display_values);
  void display_log_block_summary(DfsBroker::Client *dfs_client, String prefix, CommitLogReader *log_reader);

}


/**
 *
 */
int main(int argc, char **argv) {
  string cfgfile = "";
  string log_dir;
  PropertiesPtr props_ptr;
  bool verbose = false;
  CommPtr comm_ptr;
  ConnectionManagerPtr conn_manager_ptr;
  DfsBroker::Client *dfs_client;
  CommitLogReader *log_reader;
  bool block_summary = false;
  bool display_values = false;

  System::initialize(argv[0]);
  ReactorFactory::initialize((uint16_t)System::get_processor_count());

  for (int i=1; i<argc; i++) {
    if (!strcmp(argv[i], "--block-summary"))
      block_summary = true;
    else if (!strncmp(argv[i], "--config=", 9))
      cfgfile = &argv[i][9];
    else if (!strcmp(argv[i], "--display-values"))
      display_values = true;
    else if (!strcmp(argv[i], "--verbose") || !strcmp(argv[i], "-v"))
      verbose = true;
    else if (log_dir == "")
      log_dir = argv[i];
    else
      Usage::dump_and_exit(usage);
  }

  if (log_dir == "")
      Usage::dump_and_exit(usage);

  if (cfgfile == "")
    cfgfile = System::install_dir + "/conf/hypertable.cfg";

  props_ptr = new Properties(cfgfile);

  comm_ptr = new Comm();
  conn_manager_ptr = new ConnectionManager(comm_ptr.get());

  /**
   * Check for and connect to commit log DFS broker
   */
  {
    const char *loghost = props_ptr->get("Hypertable.RangeServer.CommitLog.DfsBroker.Host", 0);
    uint16_t logport    = props_ptr->get_int("Hypertable.RangeServer.CommitLog.DfsBroker.Port", 0);
    struct sockaddr_in addr;
    if (loghost != 0) {
      InetAddr::initialize(&addr, loghost, logport);
      dfs_client = new DfsBroker::Client(conn_manager_ptr, addr, 600);
    }
    else {
      dfs_client = new DfsBroker::Client(conn_manager_ptr, props_ptr);
    }

    if (!dfs_client->wait_for_connection(30)) {
      HT_ERROR("Unable to connect to DFS Broker, exiting...");
      exit(1);
    }
  }

  log_reader = new CommitLogReader(dfs_client, log_dir);

  if (block_summary) {
    printf("LOG %s\n", log_dir.c_str());
    display_log_block_summary(dfs_client, "", log_reader);
  }
  else
    display_log(dfs_client, "", log_reader, display_values);

  delete log_reader;

  return 0;
}

namespace {

  void display_log(DfsBroker::Client *dfs_client, String prefix, CommitLogReader *log_reader, bool display_values) {
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

      HT_EXPECT(header.check_magic(CommitLog::MAGIC_DATA), Error::FAILED_EXPECTATION);

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



  void display_log_block_summary(DfsBroker::Client *dfs_client, String prefix, CommitLogReader *log_reader) {
    CommitLogBlockInfo binfo;
    BlockCompressionHeaderCommitLog header;

    while (log_reader->next_raw_block(&binfo, &header)) {

      HT_EXPECT(header.check_magic(CommitLog::MAGIC_DATA), Error::FAILED_EXPECTATION);

      printf("%sDATA frag=\"%s\" ts=%llu start=%09llu end=%09llu ",
	     prefix.c_str(), binfo.file_fragment, 
	     (long long unsigned int)header.get_timestamp(),
	     (long long unsigned int)binfo.start_offset,
	     (long long unsigned int)binfo.end_offset);

      if (binfo.error == Error::OK) {
	printf("ztype=\"%s\" zlen=%u len=%u\n",
	       BlockCompressionCodec::get_compressor_name(header.get_compression_type()),
	       header.get_data_zlength(), header.get_data_length());
      }
      else
	printf("%serror = \"%s\"\n", prefix.c_str(), Error::get_text(binfo.error));
    }
  }

}
