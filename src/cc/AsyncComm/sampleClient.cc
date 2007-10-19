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

using namespace hypertable;

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

  ResponseHandler() : mQueue(), mMutex(), mCond() { return; }
  virtual ~ResponseHandler() { return; }

  virtual void handle(EventPtr &eventPtr) = 0;

  virtual bool GetResponse(EventPtr &eventPtr) = 0;

protected:
  queue<EventPtr>   mQueue;
  boost::mutex      mMutex;
  boost::condition  mCond;
};





/**
 * This is the dispatch handler that gets installed as the default
 * handler for the TCP connection.  It queues up responses
 * that can be fetched by the application via a call to GetResponse()
 * GetResponse() returns false when the connection is disconnected.
 */
class ResponseHandlerTCP : public ResponseHandler {

public:

  ResponseHandlerTCP() : ResponseHandler(), mConnected(false) { return; }
  
  virtual void handle(EventPtr &eventPtr) {
    boost::mutex::scoped_lock lock(mMutex);
    if (eventPtr->type == Event::CONNECTION_ESTABLISHED) {
      if (gVerbose)
	LOG_VA_INFO("Connection Established - %s", eventPtr->toString().c_str());
      mConnected = true;
      mCond.notify_one();
    }
    else if (eventPtr->type == Event::DISCONNECT) {
      if (eventPtr->error != 0) {
	LOG_VA_INFO("Disconnect : %s", Error::GetText(eventPtr->error));
      }
      else {
	LOG_INFO("Disconnect.");
      }
      mConnected = false;
      mCond.notify_one();
    }
    else if (eventPtr->type == Event::ERROR) {
      LOG_VA_INFO("Error : %s", Error::GetText(eventPtr->error));
      //exit(1);
    }
    else if (eventPtr->type == Event::MESSAGE) {
      mQueue.push(eventPtr);
      mCond.notify_one();
    }
  }

  bool WaitForConnection() {
    boost::mutex::scoped_lock lock(mMutex);
    if (mConnected)
      return true;
    mCond.wait(lock);
    return mConnected;
  }

  virtual bool GetResponse(EventPtr &eventPtr) {
    boost::mutex::scoped_lock lock(mMutex);
    while (mQueue.empty()) {
      if (mConnected == false)
	return false;
      mCond.wait(lock);
    }
    eventPtr = mQueue.front();
    mQueue.pop();
    return true;
  }

private:
  bool mConnected;
};



/**
 * This handler factory gets passed into Comm::Listen.  It
 * gets constructed with a pointer to a DispatchHandler.
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




/**
 * This is the dispatch handler that gets installed as the default
 * handler for UDP mode.  It queues up the responses which can be
 * fetched by the application via a call to GetResponse()
 */
class ResponseHandlerUDP : public ResponseHandler {

public:

  ResponseHandlerUDP() : ResponseHandler() { return; }
  
  virtual void handle(EventPtr &eventPtr) {
    boost::mutex::scoped_lock lock(mMutex);
    if (eventPtr->type == Event::MESSAGE) {
      mQueue.push(eventPtr);
      mCond.notify_one();
    }
    else {
      LOG_VA_INFO("%s", eventPtr->toString().c_str());
      //exit(1);
    }
  }

  virtual bool GetResponse(EventPtr &eventPtr) {
    boost::mutex::scoped_lock lock(mMutex);
    while (mQueue.empty()) {
      mCond.wait(lock);
    }
    eventPtr = mQueue.front();
    mQueue.pop();
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
  ResponseHandler *respHandler;
  HandlerFactory *hfactory = 0;
  bool udpMode = false;
  string line;
  int outstanding = 0;
  int maxOutstanding = 50;
  const char *str;
  struct sockaddr_in localAddr;

  memset(&localAddr, 0, sizeof(localAddr));
  
  if (argc == 1)
    Usage::DumpAndExit(usage);

  System::Initialize(argv[0]);
  ReactorFactory::Initialize(1);

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
      if (!InetAddr::Initialize(&localAddr, &argv[i][12]))
	DUMP_CORE;
    }
    else if (!strcmp(argv[i], "--verbose")) {
      gVerbose = true;
    }
    else if (inputFile == 0)
      inputFile = argv[i];
    else
      Usage::DumpAndExit(usage);
  }

  if (inputFile == 0)
    Usage::DumpAndExit(usage);

  if (!InetAddr::Initialize(&addr, host, port))
    exit(1);

  comm = new Comm();

  ifstream myfile(inputFile);

  if (!myfile.is_open()) {
    LOG_VA_ERROR("Unable to open file '%s' : %s", inputFile, strerror(errno));
    return 0;
  }

  if (udpMode) {
    assert(localAddr.sin_port == 0);
    respHandler = new ResponseHandlerUDP();
    port++;
    InetAddr::Initialize(&localAddr, INADDR_ANY, port);
    if ((error = comm->CreateDatagramReceiveSocket(&localAddr, respHandler)) != Error::OK) {
      std::string str;
      LOG_VA_ERROR("Problem creating UDP receive socket %s - %s", InetAddr::StringFormat(str, localAddr), Error::GetText(error));
      exit(1);
    }
  }
  else {
    respHandler = new ResponseHandlerTCP();

    if (localAddr.sin_port == 0) {
      if ((error = comm->Connect(addr, respHandler)) != Error::OK) {
	LOG_VA_ERROR("Comm::Connect error - %s", Error::GetText(error));
	exit(1);
      }
    }
    else {
      hfactory = new HandlerFactory(respHandler);
      if ((error = comm->Listen(localAddr, hfactory, respHandler)) != Error::OK) {
	LOG_VA_ERROR("Comm::Listen error - %s", Error::GetText(error));
	exit(1);
      }
    }
    if (!((ResponseHandlerTCP *)respHandler)->WaitForConnection())
      exit(1);

  }

  while (!myfile.eof() ) {
    getline (myfile,line);
    if (line.length() > 0) {
      hbuilder.AssignUniqueId();
      CommBufPtr cbufPtr( new CommBuf(hbuilder, Serialization::EncodedLengthString(line)) );
      cbufPtr->AppendString(line);
      int retries = 0;

      if (udpMode) {
	if ((error = comm->SendDatagram(addr, localAddr, cbufPtr)) != Error::OK) {
	  LOG_VA_ERROR("Problem sending datagram - %s", Error::GetText(error));
	  return 1;
	}
      }
      else {
	while ((error = comm->SendRequest(addr, timeout, cbufPtr, respHandler)) != Error::OK) {
	  if (error == Error::COMM_NOT_CONNECTED) {
	    if (retries == 5) {
	      LOG_ERROR("Connection timeout.");
	      return 1;
	    }
	    poll(0, 0, 1000);
	    retries++;
	  }
	  else {
	    LOG_VA_ERROR("CommEngine::SendMessage returned '%s'", Error::GetText(error));
	    return 1;
	  }
	}
      }
      outstanding++;

      if (outstanding  > maxOutstanding) {
	if (!respHandler->GetResponse(eventPtr))
	  break;
	if (!Serialization::DecodeString(&eventPtr->message, &eventPtr->messageLen, &str))
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

  while (outstanding > 0 && respHandler->GetResponse(eventPtr)) {
    if (!Serialization::DecodeString(&eventPtr->message, &eventPtr->messageLen, &str))
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


