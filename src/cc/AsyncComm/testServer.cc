/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */


#include <iostream>
#include <string>
using namespace std;

extern "C" {
#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
}

#include "AsyncComm/ConnectionHandlerFactory.h"

#include "Common/Error.h"
#include "Common/System.h"
#include "Common/SockAddrMap.h"
#include "Common/Usage.h"

#include "CallbackHandler.h"
#include "Comm.h"
#include "Event.h"
using namespace hypertable;


namespace {
  int  gDelay = 0;
  bool gVerbose = false;
  const int DEFAULT_PORT = 11255;
  const char *usage[] = {
    "usage: sampleServer [OPTIONS]",
    "",
    "OPTIONS:",
    "  --help         Display this help text and exit",
    "  --port=<n>     Specifies the port to listen on (default=11255)",
    "  --reactors=<n>  Specifies the number of reactors (default=1)",
    "  --delay=<ms>    Specifies milliseconds to wait before echoing message (default=0)",
    "  --verbose,-v    Generate verbose output",
    ""
    "This is a sample program to test the AsyncComm library.  It establishes",
    "a connection with the sampleServer and sends each line of the input file",
    "to the server.  Each reply from the server is echoed to stdout.",
    (const char *)0
  };
}



class RequestHandler : public CallbackHandler {

public:
  
  virtual void handle(Event &event) {
    if (event.type == Event::CONNECTION_ESTABLISHED) {
      LOG_INFO("Connection Established.");
    }
    else if (event.type == Event::DISCONNECT) {
      if (event.error != 0) {
	LOG_VA_INFO("Disconnect : %s", Error::GetText(event.error));
      }
      else {
	LOG_INFO("Disconnect.");
      }
    }
    else if (event.type == Event::ERROR) {
      LOG_VA_WARN("Error : %s", Error::GetText(event.error));
    }
    else if (event.type == Event::MESSAGE) {
      boost::mutex::scoped_lock lock(mMutex);
      Event *eventCopy = new Event(event);
      mQueue.push(eventCopy);
      mCond.notify_one();
    }
  }

  Event *GetRequest() {
    boost::mutex::scoped_lock lock(mMutex);
    while (mQueue.empty())
      mCond.wait(lock);
    Event *event = mQueue.front();
    poll(0, 0, gDelay);
    mQueue.pop();
    return event;
  }

private:
  queue<Event *>  mQueue;
  boost::mutex     mMutex;
  boost::condition mCond;
};



/**
 */
class HandlerFactory : public ConnectionHandlerFactory {
public:
  HandlerFactory(CallbackHandler *cb) {
    mCallback = cb;
  }
  virtual CallbackHandler *newInstance() {
    return mCallback;
  }
private:
  CallbackHandler *mCallback;
};



int main(int argc, char **argv) {
  Comm *comm;
  int rval;
  uint16_t port = DEFAULT_PORT;
  int reactorCount = 1;
  HandlerFactory *hfactory = 0;

  for (int i=1; i<argc; i++) {
    if (!strcmp(argv[i], "--help"))
      Usage::DumpAndExit(usage);
    else if (!strncmp(argv[i], "--port=", 7)) {
      rval = atoi(&argv[i][7]);
      if (rval <= 1024 || rval > 65535) {
	cerr << "Invalid port.  Must be in the range of 1024-65535." << endl;
	exit(1);
      }
      port = (uint16_t)rval;
    }
    else if (!strncmp(argv[i], "--reactors=", 11))
      reactorCount = atoi(&argv[i][11]);
    else if (!strncmp(argv[i], "--delay=", 8))
      gDelay = atoi(&argv[i][8]);
    else if (!strcmp(argv[i], "--verbose") || !strcmp(argv[i], "-v"))
      gVerbose = true;
    else
      Usage::DumpAndExit(usage);
  }

  System::Initialize(argv[0]);
  ReactorFactory::Initialize(reactorCount);

  RequestHandler *requestHandler = new RequestHandler();

  hfactory = new HandlerFactory(requestHandler);

  comm = new Comm(0);

  if (gVerbose)
    cout << "Listening on port " << port << endl;

  comm->Listen(port, hfactory, requestHandler);

  Event *event;
  CommBuf *cbuf;

  while ((event = requestHandler->GetRequest()) != 0) {
    cbuf = new CommBuf(event->header->totalLen);
    cbuf->PrependData((uint8_t *)event->header, event->header->totalLen);
    //event->Display();
    int error = comm->SendResponse(event->addr, cbuf);
    if (error != Error::OK) {
      LOG_VA_ERROR("Comm::SendResponse returned %s", Error::GetText(error));
    }
    delete event;
  }

  delete comm;
  return 0;
}  
