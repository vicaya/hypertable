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

#include <cstdlib>

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
#include "Common/TestHarness.h"
#include "Common/StringExt.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ReactorFactory.h"

#include "CommTestThreadFunction.h"

using namespace hypertable;

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
      if ((mChildPid = fork()) == 0) {
	execl("./testServer", "./testServer", DEFAULT_PORT_ARG, "--app-queue", (char *)0);
      }
      poll(0,0,2000);
    }
    ~ServerLauncher() {
      if (kill(mChildPid, 9) == -1)
	perror("kill");
    }
    private:
      pid_t mChildPid;
  };

}


int main(int argc, char **argv) {
  boost::thread  *thread1, *thread2;
  struct sockaddr_in addr;
  ServerLauncher slauncher;
  Comm *comm;
  ConnectionManagerPtr connManagerPtr;

  if (argc != 1)
    Usage::DumpAndExit(usage);

  srand(8876);

  System::Initialize(argv[0]);
  ReactorFactory::Initialize(1);

  InetAddr::Initialize(&addr, "localhost", DEFAULT_PORT);

  comm = new Comm();

  connManagerPtr = new ConnectionManager(comm);
  connManagerPtr->Add(addr, 5, "testServer");
  if (!connManagerPtr->WaitForConnection(addr, 30)) {
    LOG_ERROR("Connect error");
    return 1;
  }

  CommTestThreadFunction threadFunc(comm, addr, "/usr/share/dict/words");

  threadFunc.SetOutputFile("commTest.output.1");
  thread1 = new boost::thread(threadFunc);

  threadFunc.SetOutputFile("commTest.output.2");
  thread2 = new boost::thread(threadFunc);

  thread1->join();
  thread2->join();

  std::string tmpFile = (std::string)"/tmp/commTest" + (int)getpid();
  std::string commandStr = (std::string)"head -" + (int)MAX_MESSAGES + " /usr/share/dict/words > " + tmpFile  + " ; diff " + tmpFile + " commTest.output.1";

  if (system(commandStr.c_str()))
    return 1;

  commandStr = (std::string)"unlink " + tmpFile;
  if (system(commandStr.c_str()))
    return 1;

  if (system("diff commTest.output.1 commTest.output.2"))
    return 1;

  delete comm;
  delete thread1;
  delete thread2;

  return 0;
}
