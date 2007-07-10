/*
 * commTest.cc
 *
 * $Id$
 *
 * This file is part of Placer.
 *
 * Placer is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser Public License as published by
 * the Free Software Foundation; either version 2.1 of the License, or
 * any later version.
 *
 * Placer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser Public License for more details.
 *
 * You should have received a copy of the GNU Lesser Public License
 * along with Placer; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

extern "C" {
#include <poll.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
}

#include <boost/thread/thread.hpp>

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/TestHarness.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ReactorFactory.h"

#include "CommTestThreadFunction.h"

using namespace Placer;

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
	execl("./testServer", "./testServer", DEFAULT_PORT_ARG, (char *)0);
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

  if (argc != 1)
    Usage::DumpAndExit(usage);

  System::Initialize(argv[0]);
  ReactorFactory::Initialize(1);

  memset(&addr, 0, sizeof(struct sockaddr_in));
  {
    struct hostent *he = gethostbyname("localhost");
    if (he == 0) {
      herror("gethostbyname()");
      return 1;
    }
    memcpy(&addr.sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
  }
  addr.sin_family = AF_INET;
  addr.sin_port = htons(DEFAULT_PORT);

  comm = new Comm(0);

  ConnectionManager::Callback *connHandler = new ConnectionManager::Callback(comm, addr, 5);

  connHandler->SendConnectRequest();

  if (!connHandler->WaitForEvent(30)) {
    LOG_ERROR("Connect error");
    return 1;
  }

  CommTestThreadFunction threadFunc(comm, addr, "/usr/share/dict/words");

  threadFunc.SetOutputFile("tests/commTest.output.1");
  thread1 = new boost::thread(threadFunc);

  threadFunc.SetOutputFile("tests/commTest.output.2");
  thread2 = new boost::thread(threadFunc);

  thread1->join();
  thread2->join();

  if (system("diff /usr/share/dict/words tests/commTest.output.1"))
    return 1;

  if (system("diff tests/commTest.output.1 tests/commTest.output.2"))
    return 1;

  return 0;
}
