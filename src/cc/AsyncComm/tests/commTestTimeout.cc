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
#include "Common/Logger.h"
#include "Common/TestHarness.h"
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
      if ((mChildPid = fork()) == 0) {
	execl("./testServer", "./testServer", DEFAULT_PORT_ARG, "--delay=120000", (char *)0);
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

  /**
   * 
   */
  class ResponseHandler : public DispatchHandler {
  public:
    ResponseHandler() : mMutex(), mCond() { return; }

    virtual void handle(EventPtr &eventPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      if (eventPtr->type == Event::ERROR) {
	LOG_VA_INFO("%s", eventPtr->toString().c_str());
      }
      mCond.notify_one();
    }

    void WaitForConnection() {
      boost::mutex::scoped_lock lock(mMutex);
      mCond.wait(lock);
    }

  private:
    boost::mutex      mMutex;
    boost::condition  mCond;
  };

}


int main(int argc, char **argv) {
  boost::thread  *thread1, *thread2;
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
	Usage::DumpAndExit(usage);
    }

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

    if ((error = comm->Connect(addr, dispatchHandlerPtr)) != Error::OK)
      return 1;

    respHandler->WaitForConnection();

    HeaderBuilder hbuilder(Header::PROTOCOL_NONE, rand());
    std::string msg;

    msg = "foo";
    hbuilder.AssignUniqueId();
    CommBufPtr cbufPtr( new CommBuf(hbuilder, Serialization::EncodedLengthString(msg)) );
    cbufPtr->AppendString(msg);
    if ((error = comm->SendRequest(addr, 5, cbufPtr, respHandler)) != Error::OK) {
      LOG_VA_ERROR("Problem sending request - %s", Error::GetText(error));
      return 1;
    }

    msg = "bar";
    hbuilder.AssignUniqueId();
    cbufPtr.reset (new CommBuf(hbuilder, Serialization::EncodedLengthString(msg)) );
    cbufPtr->AppendString(msg);
    if ((error = comm->SendRequest(addr, 5, cbufPtr, respHandler)) != Error::OK) {
      LOG_VA_ERROR("Problem sending request - %s", Error::GetText(error));
      return 1;
    }

    poll(0, 0, 8000);

    delete comm;

  }

  if (!golden)
    harness.ValidateAndExit("commTestTimeout.golden");

  harness.RegenerateGoldenFile("commTestTimeout.golden");

  return 0;
}
