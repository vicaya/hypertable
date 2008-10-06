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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
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
#include "Common/Thread.h"
#include "Common/StaticBuffer.h"

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

  void test_copy(DfsBroker::Client *client, const String &testdir) {
    String outfileA = testdir + "/output.a";
    String outfileB = testdir + "/output.b";

    DfsTestThreadFunction thread_func(client, "/usr/share/dict/words");

    thread_func.set_dfs_file(outfileA);
    thread_func.set_output_file("output.a");
    Thread thread1(thread_func);

    thread_func.set_dfs_file(outfileB);
    thread_func.set_output_file("output.b");
    Thread thread2(thread_func);

    thread1.join();
    thread2.join();

    if (system("diff /usr/share/dict/words output.a"))
      exit(1);

    if (system("diff output.a output.b"))
      exit(1);
  }

  void test_readdir(DfsBroker::Client *client, const String &testdir) {
    ofstream filestr ("dfsTest.out");
    vector<string> listing;

    client->readdir(testdir, listing);

    sort(listing.begin(), listing.end());

    for (size_t i=0; i<listing.size(); i++)
      filestr << listing[i] << endl;

    filestr.close();

    if (system("diff dfsTest.out dfsTest.golden"))
      exit(1);
  }

  void test_rename(DfsBroker::Client *client, const String &testdir) {
    const char *magic = "the quick brown fox jumps over a lazy dog";
    char buf[1024];
    String file_a = testdir +"/filename.a";
    String file_b = testdir +"/filename.b";
    int fd = client->create(file_a, true, -1, -1, -1);
    StaticBuffer sbuf((char *)magic, strlen(magic) + 1, false);
    client->append(fd, sbuf);
    client->close(fd);
    client->rename(file_a, file_b);
    fd = client->open(file_b);
    client->read(fd, buf, sizeof(buf));
    HT_ASSERT(strcmp(buf, magic) == 0);
    client->close(fd);
  }
}


int main(int argc, char **argv) {
  try {
    struct sockaddr_in addr;
    ConnectionManagerPtr conn_mgr;
    DfsBroker::Client *client;

    if (argc != 1)
      Usage::dump_and_exit(usage);

    System::initialize(argv[0]);
    ReactorFactory::initialize(2);

    InetAddr::initialize(&addr, "localhost", DEFAULT_DFSBROKER_PORT);

    conn_mgr = new ConnectionManager();
    client = new DfsBroker::Client(conn_mgr, addr, 15);

    if (!client->wait_for_connection(15)) {
      HT_ERROR("Unable to connect to DFS");
      return 1;
    }
    String testdir = format("/dfsTest%d", getpid());
    client->mkdirs(testdir);

    test_copy(client, testdir);
    test_readdir(client, testdir);
    test_rename(client, testdir);

    client->rmdir(testdir);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  catch (...) {
    HT_ERROR_OUT << "unexpected exception caught" << HT_END;
    return 1;
  }
  return 0;
}
