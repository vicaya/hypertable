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

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/ServerLauncher.h"
#include "Common/System.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/CommBuf.h"
#include "AsyncComm/DispatchHandler.h"
#include "AsyncComm/HeaderBuilder.h"
#include "AsyncComm/ReactorFactory.h"


using namespace hypertable;

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

    void SetPending() { mPending = true; }
  
    virtual void handle(EventPtr &eventPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      mPending = false;
      mCond.notify_all();
    }

    void WaitForNotification() {
      boost::mutex::scoped_lock lock(mMutex);
      while (mPending)
	mCond.wait(lock);
    }

  private:
    boost::mutex      mMutex;
    boost::condition  mCond;
    bool mPending;
  };

  NotificationHandler gNotifyHandler;

  void IssueCommandNoWait(int fd, const char *command) {
    if (write(fd, command, strlen(command)) != strlen(command)) {
      perror("write");
      exit(1);
    }
    if (write(fd, "\n", 1) != 1) {
      perror("write");
      exit(1);
    }
  }

  void IssueCommand(int fd, const char *command) {
    gNotifyHandler.SetPending();
    IssueCommandNoWait(fd, command);
    gNotifyHandler.WaitForNotification();
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

  System::Initialize(argv[0]);
  ReactorFactory::Initialize(1);

  comm = new Comm();

  if (!InetAddr::Initialize(&addr, "23451"))
    exit(1);

  if ((error = comm->CreateDatagramReceiveSocket(&addr, &gNotifyHandler)) != Error::OK) {
    std::string str;
    LOG_VA_ERROR("Problem creating UDP receive socket %s - %s", InetAddr::StringFormat(str, addr), Error::GetText(error));
    exit(1);
  }

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
  clientArgs.push_back("--notification-address=23451");
  clientArgs.push_back((const char *)0);

  {
    ServerLauncher master("../../Hyperspace/Hyperspace.Master", (char * const *)&masterArgs[0]);
    poll(0, 0, 1000);
    ServerLauncher client1("./hyperspace2", (char * const *)&clientArgs[0], "client1.out");
    ServerLauncher client2("./hyperspace2", (char * const *)&clientArgs[0], "client2.out");
    ServerLauncher client3("./hyperspace2", (char * const *)&clientArgs[0], "client3.out");

    gFd1 = client1.GetWriteDescriptor();
    gFd2 = client2.GetWriteDescriptor();
    gFd3 = client3.GetWriteDescriptor();

    gPid1 = client1.GetPid();
    gPid2 = client2.GetPid();
    gPid3 = client3.GetPid();
    
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
    IssueCommand(gFd1, "delete dir1");
    IssueCommand(gFd1, "close foo");    
    IssueCommand(gFd1, "attrset foo testattr=\"Hello, World!\"");
    IssueCommand(gFd1, "attrget foo testattr");
    IssueCommand(gFd1, "attrdel foo testattr");
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
    IssueCommand(gFd2, "release lockfile");    
    IssueCommand(gFd1, "close lockfile");
    IssueCommand(gFd2, "close lockfile");
    IssueCommand(gFd3, "close lockfile");
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
    if (kill(gPid2, 17) == -1)
      perror("kill");
    poll(0, 0, 9000);
    // IssueCommand(gFd2, "close dir1/foo");
    IssueCommand(gFd1, "close dir1");
    IssueCommand(gFd1, "delete dir1");
  }

}
