/**
 * Copyright (C) 2009 Sanjit Jhala (Zvents, Inc.)
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

#include "Common/Init.h"
#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/ServerLauncher.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/CommBuf.h"
#include "AsyncComm/DispatchHandler.h"
#include "AsyncComm/ReactorFactory.h"

using namespace Hypertable;

namespace {
  const char *usage[] = {
    "usage: hypertable_refresh_schema_test.cc",
    "",
    "This program launches 3 hypertable command interpreters",
    "uses one to alter a table and the other to read/write to the table and captur the ",
    "output for diffing.",
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
      ScopedLock lock(m_mutex);
      m_pending = false;
      m_cond.notify_all();
    }

    void wait_for_notification() {
      ScopedLock lock(m_mutex);
      while (m_pending)
        m_cond.wait(lock);
    }

  private:
    Mutex             m_mutex;
    boost::condition  m_cond;
    bool m_pending;
  };

  NotificationHandler *g_notify_handler = 0;

  void IssueCommandNoWait(int fd, const char *command) {
    if (write(fd, command, strlen(command)) != (ssize_t)strlen(command)) {
      perror("write");
      exit(1);
    }
    if (write(fd, ";\n", 2) != 2) {
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

  void RunTest();
}


int main(int argc, char **argv) {
  std::vector<const char *> client_no_refresh_args;
  std::vector<const char *> client_refresh_args;
  Comm *comm;
  struct sockaddr_in addr;
  DispatchHandlerPtr dhp;

  Config::init(0, 0);

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

  client_refresh_args.push_back("hypertable");
  client_refresh_args.push_back("--config=./hypertable.cfg");
  client_refresh_args.push_back("--test-mode");
  client_refresh_args.push_back("--notification-address=23451");
  client_refresh_args.push_back((const char *)0);

  client_no_refresh_args.push_back("hypertable");
  client_no_refresh_args.push_back("--config=./hypertable.cfg");
  client_no_refresh_args.push_back("--Hypertable.Client.RefreshSchema=false");
  client_no_refresh_args.push_back("--test-mode");
  client_no_refresh_args.push_back("--notification-address=23451");
  client_no_refresh_args.push_back((const char *)0);

  {
    ServerLauncher client1("./hypertable",
                           (char * const *)&client_refresh_args[0],
                           "hypertable_refresh_schema_test_c1.out");
    ServerLauncher client2("./hypertable",
                           (char * const *)&client_refresh_args[0],
                           "hypertable_refresh_schema_test_c2.out");
    ServerLauncher client3("./hypertable",
                           (char * const *)&client_no_refresh_args[0],
                           "hypertable_refresh_schema_test_c3.out");

    g_fd1 = client1.get_write_descriptor();
    g_fd2 = client2.get_write_descriptor();
    g_fd3 = client3.get_write_descriptor();

    g_pid1 = client1.get_pid();
    g_pid2 = client2.get_pid();
    g_pid3 = client3.get_pid();

    RunTest();

    IssueCommandNoWait(g_fd1, "quit");
    IssueCommandNoWait(g_fd2, "quit");
    IssueCommandNoWait(g_fd3, "quit");
    poll(0, 0, 1000);
  }

  if (system("diff ./hypertable_refresh_schema_test_c1.out ./hypertable_refresh_schema_test_c1.golden"))
    return 1;

  if (system("diff ./hypertable_refresh_schema_test_c2.out ./hypertable_refresh_schema_test_c2.golden"))
    return 1;

  if (system("diff ./hypertable_refresh_schema_test_c3.out ./hypertable_refresh_schema_test_c3.golden"))
    return 1;

  return 0;
}


namespace {

  void RunTest() {
    IssueCommand(g_fd1, "drop table if exists refresh_schema_test");
    IssueCommand(g_fd1, "create table refresh_schema_test ('col1')");
    IssueCommand(g_fd1, "insert into refresh_schema_test values ('r1', 'col1', 'v1')");
    IssueCommand(g_fd2, "select * from refresh_schema_test");
    IssueCommand(g_fd3, "select * from refresh_schema_test");
    IssueCommand(g_fd1, "alter table refresh_schema_test add('col2')");
    IssueCommand(g_fd2, "select * from refresh_schema_test");
    IssueCommand(g_fd3, "select * from refresh_schema_test");
    IssueCommand(g_fd1, "alter table refresh_schema_test add('col3')");
    IssueCommand(g_fd2, "insert into refresh_schema_test values ('r2', 'col3', 'v2')");
    IssueCommand(g_fd3, "insert into refresh_schema_test values ('r2', 'col3', 'v3')");
    IssueCommand(g_fd1, "select * from refresh_schema_test");
  }
}

