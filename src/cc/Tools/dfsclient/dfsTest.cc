/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

extern "C" {
#include <poll.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
}

#include <boost/thread/thread.hpp>

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ReactorFactory.h"

#include "DfsBroker/Lib/Client.h"

#include "dfsTestThreadFunction.h"

using namespace Hypertable;
using namespace std;

namespace {
  const uint16_t DEFAULT_DFSBROKER_PORT = 38030;
  const char *usage[] = {
    "usage: dfsTest",
    "",
    "  This program tests the operation of the DFS and DFS broker",
    "  by copying the file /usr/share/dict/words to the DFS via the",
    "  broker, then copying it back and making sure the returned copy",
    "  matches the original.  It assumes the DFS broker is listenting",
    "  at localhost:38546",
    (const char *)0
  };
}


int main(int argc, char **argv) {
  boost::thread  *thread1, *thread2;
  struct sockaddr_in addr;
  Comm *comm;
  ConnectionManagerPtr connManagerPtr;
  DfsBroker::Client *client;
  char buf[32];
  std::string testDir, outfileA, outfileB;
  int error;

  fstream filestr ("dfsTest.out", fstream::out);

  if (argc != 1)
    Usage::dump_and_exit(usage);

  System::initialize(argv[0]);
  ReactorFactory::initialize(1);

  InetAddr::initialize(&addr, "localhost", DEFAULT_DFSBROKER_PORT);

  comm = new Comm();
  connManagerPtr = new ConnectionManager(comm);
  client = new DfsBroker::Client(connManagerPtr, addr, 15);

  if (!client->wait_for_connection(15)) {
    HT_ERROR("Unable to connect to DFS");
    return 1;
  }

  sprintf(buf, "/dfsTest%d", getpid());
  testDir = buf;
  if ((error = client->mkdirs(testDir)) != Error::OK) {
    HT_ERRORF("Problem making DFS directory '%s' - %s", testDir.c_str(), Error::get_text(error));
    exit(1);
  }
  outfileA = testDir + "/output.a";
  outfileB = testDir + "/output.b";

  dfsTestThreadFunction threadFunc(client, "/usr/share/dict/words");

  threadFunc.set_dfs_file(outfileA);
  threadFunc.set_output_file("output.a");
  thread1 = new boost::thread(threadFunc);

  threadFunc.set_dfs_file(outfileB);
  threadFunc.set_output_file("output.b");
  thread2 = new boost::thread(threadFunc);

  thread1->join();
  thread2->join();

  /**
   * Readdir test
   */
  {
    vector<string> listing;
    
    if ((error = client->readdir(testDir, listing)) != Error::OK) {
      HT_ERRORF("Problem listing DFS test directory '%s' - %s", testDir.c_str(), Error::get_text(error));
      return 1;
    }

    sort(listing.begin(), listing.end());

    for (size_t i=0; i<listing.size(); i++)
      filestr << listing[i] << endl;
  }

  filestr.close();

  if (system("diff dfsTest.out dfsTest.golden"))
    return 1;

  if ((error = client->rmdir(testDir)) != Error::OK) {
    HT_ERRORF("Problem removing DFS test directory '%s' - %s", testDir.c_str(), Error::get_text(error));
    return 1;
  }

  if (system("diff /usr/share/dict/words output.a"))
    return 1;

  if (system("diff output.a output.b"))
    return 1;

  return 0;
}
