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

extern "C" {
#include <poll.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
}

#include <boost/thread/thread.hpp>

#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/InetAddr.h"
#include "Common/TestHarness.h"
#include "Common/StringExt.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ReactorFactory.h"

#include "CommTestThreadFunction.h"

using namespace Hypertable;

namespace {
  const char *usage[] = {
    "usage: commTest",
    "",
    "This program ...",
    0
  };

  const int DEFAULT_PORT = 32998;
  const char *DEFAULT_PORT_ARG = "--port=32998";

  class ServerLauncher {
  public:
    ServerLauncher() {
      if ((m_child_pid = fork()) == 0) {
        execl("./testServer", "./testServer", DEFAULT_PORT_ARG, "--app-queue",
              (char *)0);
      }
      poll(0,0,2000);
    }
    ~ServerLauncher() {
      if (kill(m_child_pid, 9) == -1)
        perror("kill");
    }
    private:
      pid_t m_child_pid;
  };

}


int main(int argc, char **argv) {
  boost::thread  *thread1, *thread2;
  struct sockaddr_in addr;
  ServerLauncher slauncher;
  Comm *comm;
  ConnectionManagerPtr conn_mgr;

  Config::init(argc, argv);

  if (argc != 1)
    Usage::dump_and_exit(usage);

  srand(8876);

  System::initialize(System::locate_install_dir(argv[0]));
  ReactorFactory::initialize(1);

  InetAddr::initialize(&addr, "localhost", DEFAULT_PORT);

  comm = Comm::instance();
  conn_mgr = new ConnectionManager(comm);
  conn_mgr->add(addr, 5, "testServer");
  if (!conn_mgr->wait_for_connection(addr, 30000)) {
    HT_ERROR("Connect error");
    return 1;
  }

  CommTestThreadFunction thread_func(comm, addr, "./words");

  thread_func.set_output_file("commTest.output.1");
  thread1 = new boost::thread(thread_func);

  thread_func.set_output_file("commTest.output.2");
  thread2 = new boost::thread(thread_func);

  thread1->join();
  thread2->join();

  String tmp_file = (String)"/tmp/commTest" + (int)getpid();
  String cmd_str = (String)"head -" + (int)MAX_MESSAGES + " ./words > "
      + tmp_file  + " ; diff " + tmp_file + " commTest.output.1";

  if (system(cmd_str.c_str()))
    return 1;

  cmd_str = (String)"unlink " + tmp_file;
  if (system(cmd_str.c_str()))
    return 1;

  if (system("diff commTest.output.1 commTest.output.2"))
    return 1;

  ReactorFactory::destroy();

  delete thread1;
  delete thread2;

  return 0;
}
