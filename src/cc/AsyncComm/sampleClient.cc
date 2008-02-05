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

#include <queue>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <string>
using namespace std;

extern "C" {
#include <netdb.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
}

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "DispatchHandler.h"
#include "Comm.h"
#include "Event.h"
#include "HeaderBuilder.h"
#include "Serialization.h"

using namespace Hypertable;

namespace {
  const char *DEFAULT_HOST = "localhost";
  const int DEFAULT_PORT = 11255;
  const int DEFAULT_TIMEOUT = 10;
  const char *usage[] = {
    "usage: sampleClient [OPTIONS] <input-file>",
    "",
    "OPTIONS:",
    "  --host=<name>   Specifies the host to connect to (default = localhost)",
    "  --port=<n>      Specifies the port to connect to (default = 11255)",
    "  --reactors=<n>  Specifies the number of reactors (default=1)",
    "  --recv-addr=<addr>  Let the server connect to us by listening for",
    "                  connection request on <addr> (host:port).  The address",
    "                  that the server is connecting from should be the same",
    "                  as what's specified with --host and --port or the defaults.",
    "                  (TCP only)",
    "  --timeout=<t>   Specifies the connection timeout in seconds (default=10)",
    "  --verbose       Generate verbose output",
    "  --udp           Operate in UDP mode instead of TCP",
    "",
    "This is a sample program to test the AsyncComm library.  It establishes",
    "a connection with the sampleServer and sends each line of the input file",
    "to the server.  Each reply from the server is echoed to stdout.",
    (const char *)0
  };
  bool gVerbose = false;
}



/**
 * (somewhat) Abstract base class for response handlers;  Defines
 * the message queue and the mutex and condition variable to
 * protect it.
 */
class ResponseHandler : public DispatchHandler {

public:

  ResponseHandler() : m_queue(), m_mutex(), m_cond() { return; }
  virtual ~ResponseHandler() { return; }

  virtual void handle(EventPtr &eventPtr) = 0;

  virtual bool get_response(EventPtr &eventPtr) = 0;

protected:
  queue<EventPtr>   m_queue;
  boost::mutex      m_mutex;
  boost::condition  m_cond;
};





/**
 * This is the dispatch handler that gets installed as the default
 * handler for the TCP connection.  It queues up responses
 * that can be fetched by the application via a call to GetResponse()
 * GetResponse() returns false when the connection is disconnected.
 */
class ResponseHandlerTCP : public ResponseHandler {

public:

  ResponseHandlerTCP() : ResponseHandler(), m_connected(false) { return; }
  
  virtual void handle(EventPtr &eventPtr) {
    boost::mutex::scoped_lock lock(m_mutex);
    if (eventPtr->type == Event::CONNECTION_ESTABLISHED) {
      if (gVerbose)
	HT_INFOF("Connection Established - %s", eventPtr->toString().c_str());
      m_connected = true;
      m_cond.notify_one();
    }
    else if (eventPtr->type == Event::DISCONNECT) {
      if (eventPtr->error != 0) {
	HT_INFOF("Disconnect : %s", Error::get_text(eventPtr->error));
      }
      else {
	HT_INFO("Disconnect.");
      }
      m_connected = false;
      m_cond.notify_one();
    }
    else if (eventPtr->type == Event::ERROR) {
      HT_INFOF("Error : %s", Error::get_text(eventPtr->error));
      //exit(1);
    }
    else if (eventPtr->type == Event::MESSAGE) {
      m_queue.push(eventPtr);
      m_cond.notify_one();
    }
  }

  bool wait_for_connection() {
    boost::mutex::scoped_lock lock(m_mutex);
    if (m_connected)
      return true;
    m_cond.wait(lock);
    return m_connected;
  }

  virtual bool get_response(EventPtr &eventPtr) {
    boost::mutex::scoped_lock lock(m_mutex);
    while (m_queue.empty()) {
      if (m_connected == false)
	return false;
      m_cond.wait(lock);
    }
    eventPtr = m_queue.front();
    m_queue.pop();
    return true;
  }

private:
  bool m_connected;
};



/**
 * This handler factory gets passed into Comm::listen.  It
 * gets constructed with a pointer to a DispatchHandler.
 */
class HandlerFactory : public ConnectionHandlerFactory {
public:
  HandlerFactory(DispatchHandlerPtr &dhp) {
    m_dispatch_handler_ptr = dhp;
  }
  virtual void get_instance(DispatchHandlerPtr &dhp) {
    dhp = m_dispatch_handler_ptr;
  }
private:
  DispatchHandlerPtr m_dispatch_handler_ptr;
};




/**
 * This is the dispatch handler that gets installed as the default
 * handler for UDP mode.  It queues up the responses which can be
 * fetched by the application via a call to GetResponse()
 */
class ResponseHandlerUDP : public ResponseHandler {

public:

  ResponseHandlerUDP() : ResponseHandler() { return; }
  
  virtual void handle(EventPtr &eventPtr) {
    boost::mutex::scoped_lock lock(m_mutex);
    if (eventPtr->type == Event::MESSAGE) {
      m_queue.push(eventPtr);
      m_cond.notify_one();
    }
    else {
      HT_INFOF("%s", eventPtr->toString().c_str());
      //exit(1);
    }
  }

  virtual bool get_response(EventPtr &eventPtr) {
    boost::mutex::scoped_lock lock(m_mutex);
    while (m_queue.empty()) {
      m_cond.wait(lock);
    }
    eventPtr = m_queue.front();
    m_queue.pop();
    return true;
  }
};





/**
 * main function
 */
int main(int argc, char **argv) {
  Comm *comm;
  int rval;
  const char *host = DEFAULT_HOST;
  struct sockaddr_in addr;
  uint16_t port = DEFAULT_PORT;
  time_t timeout = DEFAULT_TIMEOUT;
  int reactorCount = 1;
  const char *inputFile = 0;
  HeaderBuilder hbuilder;
  int error;
  EventPtr eventPtr;
  ConnectionHandlerFactoryPtr chfPtr;
  DispatchHandlerPtr dhp;
  ResponseHandler *respHandler;
  bool udpMode = false;
  string line;
  int outstanding = 0;
  int maxOutstanding = 50;
  const char *str;
  struct sockaddr_in localAddr;

  memset(&localAddr, 0, sizeof(localAddr));
  
  if (argc == 1)
    Usage::dump_and_exit(usage);

  System::initialize(argv[0]);
  ReactorFactory::initialize(1);

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--host=", 7))
      host = &argv[i][7];
    else if (!strncmp(argv[i], "--port=", 7)) {
      rval = atoi(&argv[i][7]);
      if (rval <= 1024 || rval > 65535) {
	cerr << "Invalid port.  Must be in the range of 1024-65535." << endl;
	exit(1);
      }
      port = rval;
    }
    else if (!strncmp(argv[i], "--timeout=", 10))
      timeout = (time_t)atoi(&argv[i][10]);
    else if (!strcmp(argv[i], "--udp"))
      udpMode = true;
    else if (!strncmp(argv[i], "--reactors=", 11))
      reactorCount = atoi(&argv[i][11]);
    else if (!strncmp(argv[i], "--recv-addr=", 12)) {
      if (!InetAddr::initialize(&localAddr, &argv[i][12]))
	DUMP_CORE;
    }
    else if (!strcmp(argv[i], "--verbose")) {
      gVerbose = true;
    }
    else if (inputFile == 0)
      inputFile = argv[i];
    else
      Usage::dump_and_exit(usage);
  }

  if (inputFile == 0)
    Usage::dump_and_exit(usage);

  if (!InetAddr::initialize(&addr, host, port))
    exit(1);

  comm = new Comm();

  ifstream myfile(inputFile);

  if (!myfile.is_open()) {
    HT_ERRORF("Unable to open file '%s' : %s", inputFile, strerror(errno));
    return 0;
  }

  if (udpMode) {
    assert(localAddr.sin_port == 0);
    respHandler = new ResponseHandlerUDP();
    dhp = respHandler;
    port++;
    InetAddr::initialize(&localAddr, INADDR_ANY, port);
    if ((error = comm->create_datagram_receive_socket(&localAddr, dhp)) != Error::OK) {
      std::string str;
      HT_ERRORF("Problem creating UDP receive socket %s - %s", InetAddr::string_format(str, localAddr), Error::get_text(error));
      exit(1);
    }
  }
  else {
    respHandler = new ResponseHandlerTCP();
    dhp = respHandler;

    if (localAddr.sin_port == 0) {
      if ((error = comm->connect(addr, dhp)) != Error::OK) {
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
    if (!((ResponseHandlerTCP *)respHandler)->wait_for_connection())
      exit(1);

  }

  while (!myfile.eof() ) {
    getline (myfile,line);
    if (line.length() > 0) {
      CommBufPtr cbufPtr( new CommBuf(hbuilder, Serialization::encoded_length_string(line)) );
      cbufPtr->append_string(line);
      int retries = 0;

      if (udpMode) {
	if ((error = comm->send_datagram(addr, localAddr, cbufPtr)) != Error::OK) {
	  HT_ERRORF("Problem sending datagram - %s", Error::get_text(error));
	  return 1;
	}
      }
      else {
	while ((error = comm->send_request(addr, timeout, cbufPtr, respHandler)) != Error::OK) {
	  if (error == Error::COMM_NOT_CONNECTED) {
	    if (retries == 5) {
	      HT_ERROR("Connection timeout.");
	      return 1;
	    }
	    poll(0, 0, 1000);
	    retries++;
	  }
	  else {
	    HT_ERRORF("CommEngine::send_message returned '%s'", Error::get_text(error));
	    return 1;
	  }
	}
      }
      outstanding++;

      if (outstanding  > maxOutstanding) {
	if (!respHandler->get_response(eventPtr))
	  break;
	if (!Serialization::decode_string(&eventPtr->message, &eventPtr->messageLen, &str))
	  cout << "ERROR: deserialization problem." << endl;
	else {
	  if (*str != 0)
	    cout << "ECHO: " << str << endl;
	  else
	    cout << "[NULL]" << endl;
	}
	outstanding--;
      }
    }
  }

  while (outstanding > 0 && respHandler->get_response(eventPtr)) {
    if (!Serialization::decode_string(&eventPtr->message, &eventPtr->messageLen, &str))
      cout << "ERROR: deserialization problem." << endl;
    else {
      if (str != 0)
	cout << "ECHO: " << str << endl;
      else
	cout << "[NULL]" << endl;
    }
    //cout << "out = " << outstanding << endl;
    outstanding--;
  }


  myfile.close();

  delete comm;
  return 0;
}


