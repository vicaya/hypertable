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

extern "C" {
#include <unistd.h>
}

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/ServerLauncher.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/CommBuf.h"
#include "AsyncComm/DispatchHandler.h"
#include "AsyncComm/HeaderBuilder.h"
#include "AsyncComm/ReactorFactory.h"


using namespace Hypertable;

namespace {
  const char *usage[] = {
    "usage: hyperspaceTest",
    "",
    "This program tests Hyperspace using the hyperspace command interpreter.  It",
    "Launches a Hyperspace server configured to use ./hyperspace as it's root",
    "directory.  It then launches several hyperspace command interpreters and",
    "issues commands to them, capturing the output for diffing.",
    0
  };

  /**
   *
   */
  class NotificationHandler : public DispatchHandler {

  public:

    NotificationHandler() : DispatchHandler() { return; }

    void set_pending() { m_pending = true; }
  
    virtual void handle(EventPtr &eventPtr) {
      boost::mutex::scoped_lock lock(m_mutex);
      m_pending = false;
      m_cond.notify_all();
    }

    void wait_for_notification() {
      boost::mutex::scoped_lock lock(m_mutex);
      while (m_pending)
	m_cond.wait(lock);
    }

  private:
    boost::mutex      m_mutex;
    boost::condition  m_cond;
    bool m_pending;
  };

  NotificationHandler *gNotifyHandler = 0;

  void IssueCommandNoWait(int fd, const char *command) {
    if (write(fd, command, strlen(command)) != (ssize_t)strlen(command)) {
      perror("write");
      exit(1);
    }
    if (write(fd, "\n", 1) != 1) {
      perror("write");
      exit(1);
    }
  }

  void IssueCommand(int fd, const char *command) {
    gNotifyHandler->set_pending();
    IssueCommandNoWait(fd, command);
    gNotifyHandler->wait_for_notification();
  }

  int gFd1, gFd2, gFd3;
  pid_t gPid1, gPid2, gPid3;

  void OutputTestHeader(const char *name);
  void BasicTest();
  void NotificationTest();
  void LockTest();
  void EphemeralFileTest();
  void SessionExpirationTest();

}


int main(int argc, char **argv) {
  std::vector<const char *> masterArgs;
  std::vector<const char *> clientArgs;
  int error;
  Comm *comm;
  struct sockaddr_in addr;
  char masterInstallDir[2048];
  DispatchHandlerPtr dhp;

  if (argc > 1 && (!strcmp(argv[1], "-?") || !strcmp(argv[1], "--help")))
    Usage::dump_and_exit(usage);

  gNotifyHandler = new NotificationHandler();

  dhp = gNotifyHandler;

  System::initialize(argv[0]);
  ReactorFactory::initialize(1);

  comm = new Comm();

  if (!InetAddr::initialize(&addr, "23451"))
    exit(1);

  if ((error = comm->create_datagram_receive_socket(&addr, dhp)) != Error::OK) {
    std::string str;
    HT_ERRORF("Problem creating UDP receive socket %s - %s", InetAddr::string_format(str, addr), Error::get_text(error));
    exit(1);
  }

  system("/bin/rm -rf ./hsroot");

  if (system("mkdir ./hsroot") != 0) {
    HT_ERROR("Unable to create ./hsroot directory");
    exit(1);
  }

  strcpy(masterInstallDir, "--install-dir=");
  getcwd(masterInstallDir+strlen("--install-dir="), 2000);

  masterArgs.push_back("Hyperspace.Master");
  masterArgs.push_back("--config=./hyperspaceTest.cfg");
  masterArgs.push_back("--verbose");
  masterArgs.push_back(masterInstallDir);
  masterArgs.push_back((const char *)0);

  clientArgs.push_back("hyperspace");
  clientArgs.push_back("--config=./hyperspaceTest.cfg");
  clientArgs.push_back("--test-mode");
  clientArgs.push_back("--notification-address=23451");
  clientArgs.push_back((const char *)0);

  unlink("./Hyperspace.Master");
  link("../../Hyperspace/Hyperspace.Master", "./Hyperspace.Master");

  {
    ServerLauncher master("./Hyperspace.Master", (char * const *)&masterArgs[0]);
    ServerLauncher client1("./hyperspace", (char * const *)&clientArgs[0], "client1.out");
    ServerLauncher client2("./hyperspace", (char * const *)&clientArgs[0], "client2.out");
    ServerLauncher client3("./hyperspace", (char * const *)&clientArgs[0], "client3.out");

    gFd1 = client1.get_write_descriptor();
    gFd2 = client2.get_write_descriptor();
    gFd3 = client3.get_write_descriptor();

    gPid1 = client1.get_pid();
    gPid2 = client2.get_pid();
    gPid3 = client3.get_pid();

    BasicTest();
    NotificationTest();
    LockTest();
    EphemeralFileTest();
    SessionExpirationTest();

    IssueCommandNoWait(gFd1, "quit");
    IssueCommandNoWait(gFd2, "quit");
    IssueCommandNoWait(gFd3, "quit");
    poll(0, 0, 1000);
  }

  if (system("diff ./client1.out ./client1.golden"))
    return 1;

  if (system("diff ./client2.out ./client2.golden"))
    return 1;

  if (system("diff ./client3.out ./client3.golden"))
    return 1;

  return 0;
}


namespace {

  void OutputTestHeader(const char *name) {
    std::string commandStr = (std::string)"echo " + name;
    IssueCommand(gFd1, "echo");
    IssueCommand(gFd1, commandStr.c_str());
    IssueCommand(gFd2, "echo");
    IssueCommand(gFd2, commandStr.c_str());
    IssueCommand(gFd3, "echo");
    IssueCommand(gFd3, commandStr.c_str());
  }

  void BasicTest() {
    OutputTestHeader("<< BasicTest >>");
    IssueCommand(gFd1, "mkdir dir1");
    IssueCommand(gFd1, "mkdir how/now/brown/cow");
    IssueCommand(gFd1, "delete foo");
    IssueCommand(gFd1, "open foo flags=READ");
    IssueCommand(gFd1, "open foo flags=READ|CREATE");
    IssueCommand(gFd1, "open foo flags=READ|CREATE|EXCL");
    IssueCommand(gFd1, "lock foo EXCLUSIVE");
    IssueCommand(gFd1, "exists dir1");
    IssueCommand(gFd1, "exists foo");
    IssueCommand(gFd1, "exists bar");
    IssueCommand(gFd1, "attrget foo testattr");
    IssueCommand(gFd1, "open flags=READ /");
    IssueCommand(gFd1, "readdir /");
    IssueCommand(gFd1, "close /");
    IssueCommand(gFd1, "delete dir1");
    IssueCommand(gFd1, "close foo");
    IssueCommand(gFd1, "attrset foo testattr=\"Hello, World!\"");
    IssueCommand(gFd1, "attrget foo testattr");
    IssueCommand(gFd1, "attrdel foo testattr");
    IssueCommand(gFd1, "delete foo");
    IssueCommand(gFd1, "create foo flags=READ|WRITE attr:msg1=\"Hello, World!\" attr:msg2=\"How now brown cow\"");
    IssueCommand(gFd2, "open foo flags=READ");
    IssueCommand(gFd3, "open foo flags=READ");
    IssueCommand(gFd2, "attrget foo msg1");
    IssueCommand(gFd3, "attrget foo msg2");
    IssueCommand(gFd1, "close foo");
    IssueCommand(gFd2, "close foo");
    IssueCommand(gFd3, "close foo");
    IssueCommand(gFd1, "delete foo");
  }

  void NotificationTest() {
    OutputTestHeader("<< NotificationTest >>");
    IssueCommand(gFd1, "mkdir dir1");
    IssueCommand(gFd1, "open dir1 flags=READ|CREATE|WRITE event-mask=ATTR_SET|ATTR_DEL|CHILD_NODE_ADDED|CHILD_NODE_REMOVED|LOCK_ACQUIRED|LOCK_RELEASED");
    IssueCommand(gFd2, "mkdir dir1/foo");
    IssueCommand(gFd2, "delete dir1/foo");
    IssueCommand(gFd2, "open dir1 flags=READ|WRITE|LOCK");
    IssueCommand(gFd2, "attrset dir1 fox=\"Hello, World!\"");
    IssueCommand(gFd2, "attrget dir1 fox");
    IssueCommand(gFd2, "attrdel dir1 fox");
    IssueCommand(gFd2, "lock dir1 EXCLUSIVE");
    IssueCommand(gFd2, "release dir1");
    IssueCommand(gFd1, "close dir1");
    IssueCommand(gFd2, "close dir1");
    IssueCommand(gFd2, "delete dir1");
  }

  void LockTest() {
    OutputTestHeader("<< LockTest >>");
    IssueCommand(gFd1, "open lockfile flags=READ|CREATE|WRITE event-mask=ATTR_SET|ATTR_DEL|CHILD_NODE_ADDED|CHILD_NODE_REMOVED|LOCK_ACQUIRED|LOCK_RELEASED");
    IssueCommand(gFd2, "open lockfile flags=READ|WRITE|LOCK");
    IssueCommand(gFd3, "open lockfile flags=READ|WRITE|LOCK");
    IssueCommand(gFd2, "lock lockfile EXCLUSIVE");
    IssueCommandNoWait(gFd3, "lock lockfile EXCLUSIVE");
    poll(0,0,1000);
    IssueCommand(gFd2, "release lockfile");
    poll(0,0,1000);
    IssueCommand(gFd3, "release lockfile");
    IssueCommand(gFd2, "lock lockfile SHARED");
    IssueCommand(gFd3, "lock lockfile SHARED");
    IssueCommand(gFd2, "release lockfile");
    IssueCommand(gFd3, "release lockfile");
    IssueCommand(gFd2, "trylock lockfile EXCLUSIVE");
    IssueCommand(gFd3, "trylock lockfile EXCLUSIVE");
    IssueCommand(gFd2, "release lockfile");    
    IssueCommand(gFd3, "release lockfile");
    IssueCommand(gFd2, "close lockfile");
    IssueCommand(gFd3, "close lockfile");
    IssueCommand(gFd2, "open lockfile flags=READ|WRITE|LOCK_EXCLUSIVE");
    IssueCommand(gFd3, "open lockfile flags=READ|WRITE|LOCK_EXCLUSIVE");
    IssueCommand(gFd2, "getseq lockfile");
    IssueCommand(gFd2, "close lockfile");
    IssueCommand(gFd2, "open lockfile flags=READ|WRITE|LOCK_SHARED");
    IssueCommand(gFd3, "open lockfile flags=READ|WRITE|LOCK_SHARED");
    IssueCommand(gFd2, "getseq lockfile");
    IssueCommand(gFd2, "close lockfile");
    IssueCommand(gFd3, "close lockfile");
    IssueCommand(gFd1, "close lockfile");
    IssueCommand(gFd1, "delete lockfile");
  }

  void EphemeralFileTest() {
    OutputTestHeader("<< EphemeralFileTest >>");
    IssueCommand(gFd1, "mkdir dir1");
    IssueCommand(gFd1, "open dir1 flags=READ|CREATE|WRITE event-mask=ATTR_SET|ATTR_DEL|CHILD_NODE_ADDED|CHILD_NODE_REMOVED|LOCK_ACQUIRED|LOCK_RELEASED");
    IssueCommand(gFd2, "open dir1/foo flags=READ|CREATE|WRITE|TEMP");
    IssueCommand(gFd2, "close dir1/foo");
    IssueCommand(gFd1, "close dir1");
    IssueCommand(gFd1, "delete dir1");
  }

  void SessionExpirationTest() {
    OutputTestHeader("<< SessionExpirationTest >>");
    IssueCommand(gFd1, "mkdir dir1");
    IssueCommand(gFd1, "open dir1 flags=READ|CREATE|WRITE event-mask=ATTR_SET|ATTR_DEL|CHILD_NODE_ADDED|CHILD_NODE_REMOVED|LOCK_ACQUIRED|LOCK_RELEASED");
    IssueCommand(gFd2, "open dir1/foo flags=READ|CREATE|WRITE|TEMP");
    IssueCommand(gFd2, "open dir1 flags=READ|WRITE|LOCK");
    IssueCommand(gFd2, "lock dir1 EXCLUSIVE");
    if (kill(gPid2, SIGSTOP) == -1)
      perror("kill");
    poll(0, 0, 9000);
    // IssueCommand(gFd2, "close dir1/foo");
    IssueCommand(gFd1, "close dir1");
    IssueCommand(gFd1, "delete dir1");
  }

}
