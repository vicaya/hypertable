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

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

/**
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
#include "Common/StringExt.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ReactorFactory.h"

#include "CommTestThreadFunction.h"

*/

#include "Common/Logger.h"
#include "Common/ServerLauncher.h"
#include "Common/System.h"

using namespace hypertable;


namespace {
  const char *usage[] = {
    "usage: hyperspaceTest",
    "",
    "This program ...",
    0
  };

  const int DEFAULT_PORT = 32998;
  const char *DEFAULT_PORT_ARG = "--port=32998";

  void IssueCommand(int fd, const char *command) {
    if (write(fd, command, strlen(command)) != strlen(command)) {
      perror("write");
      exit(1);
    }
    if (write(fd, "\n", 1) != 1) {
      perror("write");
      exit(1);
    }
    /**
    std::string commandStr = (std::string)command + "\n";
    if (write(fd, commandStr.c_str(), strlen(commandStr.c_str())) < 0) {
      perror("write");
      exit(1);
    }
    poll(0, 0, 100);
    **/
  }

}


int main(int argc, char **argv) {
  std::vector<const char *> masterArgs;
  std::vector<const char *> clientArgs;
  int client1fd, client2fd, client3fd;

  System::Initialize(argv[0]);

  system("/bin/rm -rf ./hyperspace");

  if (system("mkdir ./hyperspace") != 0) {
    LOG_ERROR("Unable to create ./hyperspace directory");
    exit(1);
  }

  masterArgs.push_back("Hyperspace.Master");
  masterArgs.push_back("--config=./hyperspaceTest.cfg");
  masterArgs.push_back("--verbose");
  masterArgs.push_back((const char *)0);

  clientArgs.push_back("hyperspace2");
  clientArgs.push_back("--config=./hyperspaceTest.cfg");
  clientArgs.push_back("--test-mode");
  clientArgs.push_back((const char *)0);

  {
    ServerLauncher master("../../Hyperspace/Hyperspace.Master", (char * const *)&masterArgs[0]);
    ServerLauncher client1("./hyperspace2", (char * const *)&clientArgs[0], "client1.out");
    ServerLauncher client2("./hyperspace2", (char * const *)&clientArgs[0], "client2.out");

    client1fd = client1.GetWriteDescriptor();
    client2fd = client2.GetWriteDescriptor();

    IssueCommand(client1fd, "mkdir foo");
    //IssueCommand(client1fd, "open foo flags=READ|CREATE|WRITE event-mask=ATTR_SET|ATTR_DEL|CHILD_NODE_ADDED|CHILD_NODE_REMOVED|LOCK_ACQUIRED|LOCK_RELEASED");
    IssueCommand(client1fd, "open foo flags=READ|CREATE|WRITE event-mask=ATTR_SET|ATTR_DEL|CHILD_NODE_ADDED|CHILD_NODE_REMOVED|LOCK_ACQUIRED|LOCK_RELEASED");
    poll(0, 0, 2000);

    IssueCommand(client2fd, "open foo flags=READ");
    IssueCommand(client2fd, "mkdir foo/bar");
    IssueCommand(client2fd, "delete foo/bar");
    IssueCommand(client2fd, "attrset foo fox=\"Hello, World!\"");
    IssueCommand(client2fd, "attrget foo fox");
    IssueCommand(client2fd, "attrdel foo fox");
    IssueCommand(client2fd, "attrget foo fox");

    poll(0, 0, 1000);
    IssueCommand(client2fd, "quit");
    IssueCommand(client1fd, "quit");
    poll(0, 0, 1000);
  }

  if (system("diff ./client1.out ./client1.golden"))
    return 1;

  if (system("diff ./client2.out ./client2.golden"))
    return 1;

  return 0;
  

#if 0

  if (argc != 1)
    Usage::DumpAndExit(usage);

  srand(8876);

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

  comm = new Comm();

  connManager = new ConnectionManager(comm);
  connManager->Add(addr, 5, "testServer");
  if (!connManager->WaitForConnection(addr, 30)) {
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

  delete connManager;
  delete comm;
  delete thread1;
  delete thread2;

  return 0;
#endif
}
