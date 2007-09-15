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

#include <iostream>
#include <queue>
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

#include "AsyncComm/ApplicationHandler.h"
#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/ConnectionHandlerFactory.h"
#include "AsyncComm/HeaderBuilder.h"

#include "Common/Error.h"
#include "Common/System.h"
#include "Common/SockAddrMap.h"
#include "Common/Usage.h"

#include "DispatchHandler.h"
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
    "  --help          Display this help text and exit",
    "  --port=<n>      Specifies the port to listen on (default=11255)",
    "  --app-queue     Use an application queue for handling requests",
    "  --reactors=<n>  Specifies the number of reactors (default=1)",
    "  --delay=<ms>    Specifies milliseconds to wait before echoing message (default=0)",
    "  --udp           Operate in UDP mode instead of TCP",
    "  --verbose,-v    Generate verbose output",
    ""
    "This is a sample program to test the AsyncComm library.  It establishes",
    "a connection with the sampleServer and sends each line of the input file",
    "to the server.  Each reply from the server is echoed to stdout.",
    (const char *)0
  };
}

class RequestHandler : public ApplicationHandler {
public:

  RequestHandler(Comm *comm, EventPtr &eventPtr) : ApplicationHandler(eventPtr), mComm(comm) { return; }

  virtual void run() {
    mHeaderBuilder.InitializeFromRequest(mEventPtr->header);
    CommBufPtr cbufPtr( new CommBuf(mHeaderBuilder, mEventPtr->messageLen) );
    cbufPtr->AppendBytes((uint8_t *)mEventPtr->message, mEventPtr->messageLen);
    int error = mComm->SendResponse(mEventPtr->addr, cbufPtr);
    if (error != Error::OK) {
      LOG_VA_ERROR("Comm::SendResponse returned %s", Error::GetText(error));
    }
    mEventPtr.reset();
  }
private:
  Comm *mComm;
  HeaderBuilder mHeaderBuilder;
};

class Dispatcher : public DispatchHandler {

public:

  Dispatcher(Comm *comm, ApplicationQueue *appQueue) : mComm(comm), mAppQueue(appQueue) { return; }
  
  virtual void handle(EventPtr &eventPtr) {
    if (gVerbose && eventPtr->type == Event::CONNECTION_ESTABLISHED) {
      LOG_INFO("Connection Established.");
    }
    else if (gVerbose && eventPtr->type == Event::DISCONNECT) {
      if (eventPtr->error != 0) {
	LOG_VA_INFO("Disconnect : %s", Error::GetText(eventPtr->error));
      }
      else {
	LOG_INFO("Disconnect.");
      }
    }
    else if (eventPtr->type == Event::ERROR) {
      LOG_VA_WARN("Error : %s", Error::GetText(eventPtr->error));
    }
    else if (eventPtr->type == Event::MESSAGE) {
      if (mAppQueue == 0) {
	mHeaderBuilder.InitializeFromRequest(eventPtr->header);
	CommBufPtr cbufPtr( new CommBuf(mHeaderBuilder, eventPtr->messageLen) );
	cbufPtr->AppendBytes((uint8_t *)eventPtr->message, eventPtr->messageLen);
	int error = mComm->SendResponse(eventPtr->addr, cbufPtr);
	if (error != Error::OK) {
	  LOG_VA_ERROR("Comm::SendResponse returned %s", Error::GetText(error));
	}
	eventPtr.reset();
      }
      else {
	ApplicationHandlerPtr appHandlerPtr( new RequestHandler(mComm, eventPtr) );
	mAppQueue->Add(appHandlerPtr);
      }
    }
  }

private:
  Comm             *mComm;
  ApplicationQueue *mAppQueue;
  HeaderBuilder     mHeaderBuilder;
};



/**
 *
 */
class HandlerFactory : public ConnectionHandlerFactory {
public:
  HandlerFactory(DispatchHandler *dh) {
    mDispatchHandler = dh;
  }
  virtual DispatchHandler *newInstance() {
    return mDispatchHandler;
  }
private:
  DispatchHandler *mDispatchHandler;
};


class UdpDispatcher : public DispatchHandler {
public:

  UdpDispatcher(Comm *comm) : mComm(comm) { return; }

  virtual void handle(EventPtr &eventPtr) {
    if (eventPtr->type == Event::MESSAGE) {
      mHeaderBuilder.InitializeFromRequest(eventPtr->header);
      CommBufPtr cbufPtr( new CommBuf(mHeaderBuilder, eventPtr->messageLen) );
      cbufPtr->AppendBytes((uint8_t *)eventPtr->message, eventPtr->messageLen);
      int error = mComm->SendDatagram(eventPtr->addr, eventPtr->receivePort, cbufPtr);
      if (error != Error::OK) {
	LOG_VA_ERROR("Comm::SendResponse returned %s", Error::GetText(error));
      }
      eventPtr.reset();
    }
    else {
      LOG_VA_ERROR("Error : %s", eventPtr->toString().c_str());
    }
  }

private:
  Comm           *mComm;
  HeaderBuilder   mHeaderBuilder;
};



int main(int argc, char **argv) {
  Comm *comm;
  int rval, error;
  uint16_t port = DEFAULT_PORT;
  int reactorCount = 2;
  HandlerFactory *hfactory = 0;
  ApplicationQueue *appQueue = 0;
  bool udp = false;
  Dispatcher *dispatcher = 0;
  UdpDispatcher *udpDispatcher = 0;

  for (int i=1; i<argc; i++) {
    if (!strcmp(argv[i], "--help"))
      Usage::DumpAndExit(usage);
    else if (!strcmp(argv[i], "--app-queue")) {
      appQueue = new ApplicationQueue(5);
    }
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
    else if (!strcmp(argv[i], "--udp"))
      udp = true;
    else if (!strcmp(argv[i], "--verbose") || !strcmp(argv[i], "-v"))
      gVerbose = true;
    else
      Usage::DumpAndExit(usage);
  }

  System::Initialize(argv[0]);
  ReactorFactory::Initialize(reactorCount);

  comm = new Comm();

  if (gVerbose)
    cout << "Listening on port " << port << endl;

  if (!udp) {

    dispatcher = new Dispatcher(comm, appQueue);

    hfactory = new HandlerFactory(dispatcher);

    error = comm->Listen(port, hfactory, dispatcher);

  }
  else {

    udpDispatcher = new UdpDispatcher(comm);

    error = comm->OpenDatagramReceivePort(port, udpDispatcher);
    
  }

  poll(0, 0, -1);

  delete hfactory;
  delete dispatcher;
  delete udpDispatcher;
  delete comm;
  return 0;
}  
