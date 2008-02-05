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
#include "Common/InetAddr.h"
#include "Common/System.h"
#include "Common/SockAddrMap.h"
#include "Common/Usage.h"

#include "DispatchHandler.h"
#include "Comm.h"
#include "Event.h"
using namespace Hypertable;


namespace {

  int  gDelay = 0;
  bool gVerbose = false;
  const int DEFAULT_PORT = 11255;
  const char *usage[] = {
    "usage: sampleServer [OPTIONS]",
    "",
    "OPTIONS:",
    "  --connect-to=<addr>  Connect to a client listening on <addr> (TCP only)."
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

  /**
   * This is the request handler class that is used when the server
   * is run with an application queue (--app-queue).  It just echos
   * back the message
   */
  class RequestHandler : public ApplicationHandler {
  public:

    RequestHandler(Comm *comm, EventPtr &eventPtr) : ApplicationHandler(eventPtr), m_comm(comm) { return; }

    virtual void run() {
      m_header_builder.initialize_from_request(m_event_ptr->header);
      CommBufPtr cbufPtr( new CommBuf(m_header_builder, m_event_ptr->messageLen) );
      cbufPtr->append_bytes((uint8_t *)m_event_ptr->message, m_event_ptr->messageLen);
      int error = m_comm->send_response(m_event_ptr->addr, cbufPtr);
      if (error != Error::OK) {
	HT_ERRORF("Comm::send_response returned %s", Error::get_text(error));
      }
    }
  private:
    Comm *m_comm;
    HeaderBuilder m_header_builder;
  };


  /**
   * This is the dispatch handler that is used when
   * the server is in TCP mode (default).
   */
  class Dispatcher : public DispatchHandler {

  public:

    Dispatcher(Comm *comm, ApplicationQueue *appQueue) : m_comm(comm), m_app_queue(appQueue) { return; }
  
    virtual void handle(EventPtr &eventPtr) {
      if (gVerbose && eventPtr->type == Event::CONNECTION_ESTABLISHED) {
	HT_INFO("Connection Established.");
      }
      else if (gVerbose && eventPtr->type == Event::DISCONNECT) {
	if (eventPtr->error != 0) {
	  HT_INFOF("Disconnect : %s", Error::get_text(eventPtr->error));
	}
	else {
	  HT_INFO("Disconnect.");
	}
      }
      else if (eventPtr->type == Event::ERROR) {
	HT_WARNF("Error : %s", Error::get_text(eventPtr->error));
      }
      else if (eventPtr->type == Event::MESSAGE) {
	if (m_app_queue == 0) {
	  m_header_builder.initialize_from_request(eventPtr->header);
	  CommBufPtr cbufPtr( new CommBuf(m_header_builder, eventPtr->messageLen) );
	  cbufPtr->append_bytes((uint8_t *)eventPtr->message, eventPtr->messageLen);
	  if (gDelay > 0)
	    poll(0, 0, gDelay);
	  int error = m_comm->send_response(eventPtr->addr, cbufPtr);
	  if (error != Error::OK) {
	    HT_ERRORF("Comm::send_response returned %s", Error::get_text(error));
	  }
	}
	else
	  m_app_queue->add( new RequestHandler(m_comm, eventPtr) );
      }
    }

  private:
    Comm             *m_comm;
    ApplicationQueue *m_app_queue;
    HeaderBuilder     m_header_builder;
  };



  /**
   * This is the dispatch handler that is used when
   * the server is in UDP mode.
   */
  class UdpDispatcher : public DispatchHandler {
  public:

    UdpDispatcher(Comm *comm) : m_comm(comm) { return; }

    virtual void handle(EventPtr &eventPtr) {
      if (eventPtr->type == Event::MESSAGE) {
	m_header_builder.initialize_from_request(eventPtr->header);
	CommBufPtr cbufPtr( new CommBuf(m_header_builder, eventPtr->messageLen) );
	cbufPtr->append_bytes((uint8_t *)eventPtr->message, eventPtr->messageLen);
	if (gDelay > 0)
	  poll(0, 0, gDelay);
	int error = m_comm->send_datagram(eventPtr->addr, eventPtr->localAddr, cbufPtr);
	if (error != Error::OK) {
	  HT_ERRORF("Comm::send_response returned %s", Error::get_text(error));
	}
      }
      else {
	HT_ERRORF("Error : %s", eventPtr->toString().c_str());
      }
    }

  private:
    Comm           *m_comm;
    HeaderBuilder   m_header_builder;
  };


  /**
   * This handler factory gets passed into Comm::listen.  It
   * gets constructed with a pointer to a DispatchHandler.
   */
  class HandlerFactory : public ConnectionHandlerFactory {
  public:
    HandlerFactory(DispatchHandlerPtr &dhp) : m_dispatch_handler_ptr(dhp) { return; }

    virtual void get_instance(DispatchHandlerPtr &dhp) {
      dhp = m_dispatch_handler_ptr;
    }

  private:
    DispatchHandlerPtr m_dispatch_handler_ptr;
  };

}


/**
 * main function
 */
int main(int argc, char **argv) {
  Comm *comm;
  int rval, error;
  uint16_t port = DEFAULT_PORT;
  int reactorCount = 2;
  ConnectionHandlerFactoryPtr chfPtr;
  ApplicationQueue *appQueue = 0;
  bool udp = false;
  DispatchHandlerPtr dhp;
  struct sockaddr_in localAddr;
  struct sockaddr_in clientAddr;

  memset(&clientAddr, 0, sizeof(clientAddr));

  for (int i=1; i<argc; i++) {
    if (!strcmp(argv[i], "--help"))
      Usage::dump_and_exit(usage);
    else if (!strcmp(argv[i], "--app-queue")) {
      appQueue = new ApplicationQueue(5);
    }
    else if (!strncmp(argv[i], "--connect-to=", 13)) {
      if (!InetAddr::initialize(&clientAddr, &argv[i][13]))
	DUMP_CORE;
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
      Usage::dump_and_exit(usage);
  }

  System::initialize(argv[0]);
  ReactorFactory::initialize(reactorCount);

  comm = new Comm();

  if (gVerbose) {
    cout << "Listening on port " << port << endl;
    if (gDelay)
      cout << "Delay = " << gDelay << endl;
  }

  InetAddr::initialize(&localAddr, "localhost", port);

  if (!udp) {
    dhp = new Dispatcher(comm, appQueue);
    
    if (clientAddr.sin_port != 0) {
      if ((error = comm->connect(clientAddr, localAddr, dhp)) != Error::OK) {
	HT_ERRORF("Comm::connect error - %s", Error::get_text(error));
	exit(1);
      }
    }
    else {
      chfPtr = new HandlerFactory(dhp);
      if ((error = comm->listen(localAddr, chfPtr, dhp)) != Error::OK) {
	HT_ERRORF("Comm::listen error - %s", Error::get_text(error));
	exit(1);
      }
    }
  }
  else {
    assert(clientAddr.sin_port == 0);
    dhp = new UdpDispatcher(comm);
    error = comm->create_datagram_receive_socket(&localAddr, dhp);
  }

  poll(0, 0, -1);

  delete comm;
  return 0;
}  
