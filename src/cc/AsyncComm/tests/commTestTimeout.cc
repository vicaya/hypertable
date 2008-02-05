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
#include "Common/Logger.h"
#include "Common/TestHarness.h"
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
    "usage: commTestTimout",
    "",
    "This program tests tests the request timeout logic of AsyncComm.",
    0
  };

  const int DEFAULT_PORT = 32998;
  const char *DEFAULT_PORT_ARG = "--port=32998";

  class ServerLauncher {
  public:
    ServerLauncher() {
      if ((m_child_pid = fork()) == 0) {
	execl("./testServer", "./testServer", DEFAULT_PORT_ARG, "--delay=120000", (char *)0);
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

  /**
   * 
   */
  class ResponseHandler : public DispatchHandler {
  public:
    ResponseHandler() : m_mutex(), m_cond(), m_connected(false) { return; }

    virtual void handle(EventPtr &eventPtr) {
      boost::mutex::scoped_lock lock(m_mutex);
      if (eventPtr->type == Event::CONNECTION_ESTABLISHED) {
	m_connected = true;
	m_cond.notify_one();
      }
      else if (eventPtr->type == Event::ERROR) {
	HT_INFOF("%s", eventPtr->toString().c_str());
      }
      else {
	HT_INFOF("%s", eventPtr->toString().c_str());
	m_connected = true;
	m_cond.notify_one();
      }
    }

    void wait_for_connection() {
      boost::mutex::scoped_lock lock(m_mutex);
      while (!m_connected)
	m_cond.wait(lock);
    }

  private:
    boost::mutex      m_mutex;
    boost::condition  m_cond;
    bool              m_connected;
  };

}


int main(int argc, char **argv) {
  struct sockaddr_in addr;
  Comm *comm;
  int error;
  EventPtr eventPtr;
  TestHarness harness("commTestTimeout");
  bool golden = false;
  ResponseHandler *respHandler = new ResponseHandler();
  DispatchHandlerPtr dispatchHandlerPtr(respHandler);

  {
    ServerLauncher slauncher;

    if (argc > 1) {
      if (!strcmp(argv[1], "--golden"))
	golden = true;
      else
	Usage::dump_and_exit(usage);
    }

    srand(8876);

    System::initialize(argv[0]);
    ReactorFactory::initialize(1);

    InetAddr::initialize(&addr, "localhost", DEFAULT_PORT);

    comm = new Comm();

    if ((error = comm->connect(addr, dispatchHandlerPtr)) != Error::OK)
      return 1;

    respHandler->wait_for_connection();

    HeaderBuilder hbuilder(Header::PROTOCOL_NONE, rand());
    std::string msg;

    msg = "foo";
    CommBufPtr cbufPtr( new CommBuf(hbuilder, Serialization::encoded_length_string(msg)) );
    cbufPtr->append_string(msg);
    if ((error = comm->send_request(addr, 5, cbufPtr, respHandler)) != Error::OK) {
      HT_ERRORF("Problem sending request - %s", Error::get_text(error));
      return 1;
    }

    msg = "bar";
    cbufPtr.reset (new CommBuf(hbuilder, Serialization::encoded_length_string(msg)) );
    cbufPtr->append_string(msg);
    if ((error = comm->send_request(addr, 5, cbufPtr, respHandler)) != Error::OK) {
      HT_ERRORF("Problem sending request - %s", Error::get_text(error));
      return 1;
    }

    poll(0, 0, 8000);

    delete comm;

  }

  if (!golden)
    harness.validate_and_exit("commTestTimeout.golden");

  harness.regenerate_golden_file("commTestTimeout.golden");

  return 0;
}
