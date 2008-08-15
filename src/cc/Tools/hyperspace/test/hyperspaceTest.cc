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
    "This program tests Hyperspace using the hyperspace command interpreter.",
    "It launches a Hyperspace server configured to use ./hyperspace as its",
    "root directory.  It then launches several hyperspace command interpreters",
    "and issues commands to them, capturing the output for diffing.",
    0
  };

  /**
   *
   */
  class NotificationHandler : public DispatchHandler {

  public:

    NotificationHandler() : DispatchHandler() { return; }

    void set_pending() { m_pending = true; }

    virtual void handle(EventPtr &event_ptr) {
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

  NotificationHandler *g_notify_handler = 0;

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
    g_notify_handler->set_pending();
    IssueCommandNoWait(fd, command);
    g_notify_handler->wait_for_notification();
  }

  int g_fd1, g_fd2, g_fd3;
  pid_t g_pid1, g_pid2, g_pid3;

  void OutputTestHeader(const char *name);
  void BasicTest();
  void NotificationTest();
  void LockTest();
  void EphemeralFileTest();
  void SessionExpirationTest();

}


int main(int argc, char **argv) {
  std::vector<const char *> master_args;
  std::vector<const char *> client_args;
  Comm *comm;
  struct sockaddr_in addr;
  char master_install_dir[2048];
  DispatchHandlerPtr dhp;

  if (argc > 1 && (!strcmp(argv[1], "-?") || !strcmp(argv[1], "--help")))
    Usage::dump_and_exit(usage);

  g_notify_handler = new NotificationHandler();

  dhp = g_notify_handler;

  System::initialize(argv[0]);
  ReactorFactory::initialize(1);

  comm = Comm::instance();

  if (!InetAddr::initialize(&addr, "23451"))
    exit(1);

  comm->create_datagram_receive_socket(&addr, 0x10, dhp);

  system("/bin/rm -rf ./hsroot");

  if (system("mkdir ./hsroot") != 0) {
    HT_ERROR("Unable to create ./hsroot directory");
    exit(1);
  }

  strcpy(master_install_dir, "--install-dir=");
  getcwd(master_install_dir+strlen("--install-dir="), 2000);

  master_args.push_back("Hyperspace.Master");
  master_args.push_back("--config=./hyperspaceTest.cfg");
  master_args.push_back("--verbose");
  master_args.push_back(master_install_dir);
  master_args.push_back((const char *)0);

  client_args.push_back("hyperspace");
  client_args.push_back("--config=./hyperspaceTest.cfg");
  client_args.push_back("--test-mode");
  client_args.push_back("--notification-address=23451");
  client_args.push_back((const char *)0);

  unlink("./Hyperspace.Master");
  link("../../Hyperspace/Hyperspace.Master", "./Hyperspace.Master");

  {
    ServerLauncher master("./Hyperspace.Master",
                          (char * const *)&master_args[0]);
    ServerLauncher client1("./hyperspace",
                           (char * const *)&client_args[0], "client1.out");
    ServerLauncher client2("./hyperspace",
                           (char * const *)&client_args[0], "client2.out");
    ServerLauncher client3("./hyperspace",
                           (char * const *)&client_args[0], "client3.out");

    g_fd1 = client1.get_write_descriptor();
    g_fd2 = client2.get_write_descriptor();
    g_fd3 = client3.get_write_descriptor();

    g_pid1 = client1.get_pid();
    g_pid2 = client2.get_pid();
    g_pid3 = client3.get_pid();

    BasicTest();
    NotificationTest();
    LockTest();
    EphemeralFileTest();
    SessionExpirationTest();

    IssueCommandNoWait(g_fd1, "quit");
    IssueCommandNoWait(g_fd2, "quit");
    IssueCommandNoWait(g_fd3, "quit");
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
    std::string cmd_str = (std::string)"echo " + name;
    IssueCommand(g_fd1, "echo");
    IssueCommand(g_fd1, cmd_str.c_str());
    IssueCommand(g_fd2, "echo");
    IssueCommand(g_fd2, cmd_str.c_str());
    IssueCommand(g_fd3, "echo");
    IssueCommand(g_fd3, cmd_str.c_str());
  }

  void BasicTest() {
    OutputTestHeader("<< BasicTest >>");
    IssueCommand(g_fd1, "mkdir dir1");
    IssueCommand(g_fd1, "mkdir how/now/brown/cow");
    IssueCommand(g_fd1, "delete foo");
    IssueCommand(g_fd1, "open foo flags=READ");
    IssueCommand(g_fd1, "open foo flags=READ|CREATE");
    IssueCommand(g_fd1, "open foo flags=READ|CREATE|EXCL");
    IssueCommand(g_fd1, "lock foo EXCLUSIVE");
    IssueCommand(g_fd1, "exists dir1");
    IssueCommand(g_fd1, "exists foo");
    IssueCommand(g_fd1, "exists bar");
    IssueCommand(g_fd1, "attrget foo testattr");
    IssueCommand(g_fd1, "open flags=READ /");
    IssueCommand(g_fd1, "readdir /");
    IssueCommand(g_fd1, "close /");
    IssueCommand(g_fd1, "delete dir1");
    IssueCommand(g_fd1, "close foo");
    IssueCommand(g_fd1, "attrset foo testattr=\"Hello, World!\"");
    IssueCommand(g_fd1, "attrget foo testattr");
    IssueCommand(g_fd1, "attrdel foo testattr");
    IssueCommand(g_fd1, "delete foo");
    IssueCommand(g_fd1, "create foo flags=READ|WRITE "
        "attr:msg1=\"Hello, World!\" attr:msg2=\"How now brown cow\"");
    IssueCommand(g_fd2, "open foo flags=READ");
    IssueCommand(g_fd3, "open foo flags=READ");
    IssueCommand(g_fd2, "attrget foo msg1");
    IssueCommand(g_fd3, "attrget foo msg2");
    IssueCommand(g_fd1, "close foo");
    IssueCommand(g_fd2, "close foo");
    IssueCommand(g_fd3, "close foo");
    IssueCommand(g_fd1, "delete foo");
  }

  void NotificationTest() {
    OutputTestHeader("<< NotificationTest >>");
    IssueCommand(g_fd1, "mkdir dir1");
    IssueCommand(g_fd1, "open dir1 flags=READ|CREATE|WRITE "
        "event-mask=ATTR_SET|ATTR_DEL|CHILD_NODE_ADDED|CHILD_NODE_REMOVED"
        "|LOCK_ACQUIRED|LOCK_RELEASED");
    IssueCommand(g_fd2, "mkdir dir1/foo");
    IssueCommand(g_fd2, "delete dir1/foo");
    IssueCommand(g_fd2, "open dir1 flags=READ|WRITE|LOCK");
    IssueCommand(g_fd2, "attrset dir1 fox=\"Hello, World!\"");
    IssueCommand(g_fd2, "attrget dir1 fox");
    IssueCommand(g_fd2, "attrdel dir1 fox");
    IssueCommand(g_fd2, "lock dir1 EXCLUSIVE");
    IssueCommand(g_fd2, "release dir1");
    IssueCommand(g_fd1, "close dir1");
    IssueCommand(g_fd2, "close dir1");
    IssueCommand(g_fd2, "delete dir1");
  }

  void LockTest() {
    OutputTestHeader("<< LockTest >>");
    IssueCommand(g_fd1, "open lockfile flags=READ|CREATE|WRITE "
        "event-mask=ATTR_SET|ATTR_DEL|CHILD_NODE_ADDED|CHILD_NODE_REMOVED"
        "|LOCK_ACQUIRED|LOCK_RELEASED");
    IssueCommand(g_fd2, "open lockfile flags=READ|WRITE|LOCK");
    IssueCommand(g_fd3, "open lockfile flags=READ|WRITE|LOCK");
    IssueCommand(g_fd2, "lock lockfile EXCLUSIVE");
    IssueCommandNoWait(g_fd3, "lock lockfile EXCLUSIVE");
    poll(0,0,1000);
    IssueCommand(g_fd2, "release lockfile");
    poll(0,0,1000);
    IssueCommand(g_fd3, "release lockfile");
    IssueCommand(g_fd2, "lock lockfile SHARED");
    IssueCommand(g_fd3, "lock lockfile SHARED");
    IssueCommand(g_fd2, "release lockfile");
    IssueCommand(g_fd3, "release lockfile");
    IssueCommand(g_fd2, "trylock lockfile EXCLUSIVE");
    IssueCommand(g_fd3, "trylock lockfile EXCLUSIVE");
    IssueCommand(g_fd2, "release lockfile");
    IssueCommand(g_fd3, "release lockfile");
    IssueCommand(g_fd2, "close lockfile");
    IssueCommand(g_fd3, "close lockfile");
    IssueCommand(g_fd2, "open lockfile flags=READ|WRITE|LOCK_EXCLUSIVE");
    IssueCommand(g_fd3, "open lockfile flags=READ|WRITE|LOCK_EXCLUSIVE");
    IssueCommand(g_fd2, "getseq lockfile");
    IssueCommand(g_fd2, "close lockfile");
    IssueCommand(g_fd2, "open lockfile flags=READ|WRITE|LOCK_SHARED");
    IssueCommand(g_fd3, "open lockfile flags=READ|WRITE|LOCK_SHARED");
    IssueCommand(g_fd2, "getseq lockfile");
    IssueCommand(g_fd2, "close lockfile");
    IssueCommand(g_fd3, "close lockfile");
    IssueCommand(g_fd1, "close lockfile");
    IssueCommand(g_fd1, "delete lockfile");
  }

  void EphemeralFileTest() {
    OutputTestHeader("<< EphemeralFileTest >>");
    IssueCommand(g_fd1, "mkdir dir1");
    IssueCommand(g_fd1, "open dir1 flags=READ|CREATE|WRITE "
        "event-mask=ATTR_SET|ATTR_DEL|CHILD_NODE_ADDED|CHILD_NODE_REMOVED"
        "|LOCK_ACQUIRED|LOCK_RELEASED");
    IssueCommand(g_fd2, "open dir1/foo flags=READ|CREATE|WRITE|TEMP");
    IssueCommand(g_fd2, "close dir1/foo");
    IssueCommand(g_fd1, "close dir1");
    IssueCommand(g_fd1, "delete dir1");
  }

  void SessionExpirationTest() {
    OutputTestHeader("<< SessionExpirationTest >>");
    IssueCommand(g_fd1, "mkdir dir1");
    IssueCommand(g_fd1, "open dir1 flags=READ|CREATE|WRITE "
        "event-mask=ATTR_SET|ATTR_DEL|CHILD_NODE_ADDED|CHILD_NODE_REMOVED"
        "|LOCK_ACQUIRED|LOCK_RELEASED");
    IssueCommand(g_fd2, "open dir1/foo flags=READ|CREATE|WRITE|TEMP");
    IssueCommand(g_fd2, "open dir1 flags=READ|WRITE|LOCK");
    IssueCommand(g_fd2, "lock dir1 EXCLUSIVE");
    if (kill(g_pid2, SIGSTOP) == -1)
      perror("kill");
    poll(0, 0, 9000);
    // IssueCommand(g_fd2, "close dir1/foo");
    IssueCommand(g_fd1, "close dir1");
    IssueCommand(g_fd1, "delete dir1");
  }

}
