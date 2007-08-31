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
    "  --timeout=<t>   Specifies the connection timeout in seconds (default=10)",
    "  --reactors=<n>  Specifies the number of reactors (default=1)",
    "",
    "This is a sample program to test the AsyncComm library.  It establishes",
    "a connection with the sampleServer and sends each line of the input file",
    "to the server.  Each reply from the server is echoed to stdout.",
    (const char *)0
  };
}


class ResponseHandler : public DispatchHandler {

public:

  ResponseHandler() : mQueue(), mMutex(), mCond(), mConnected(false) { return; }
  
  virtual void handle(EventPtr &eventPtr) {
    boost::mutex::scoped_lock lock(mMutex);
    if (eventPtr->type == Event::CONNECTION_ESTABLISHED) {
      LOG_INFO("Connection Established.");
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

  bool GetResponse(EventPtr &eventPtr) {
    boost::mutex::scoped_lock lock(mMutex);
    while (mQueue.empty()) {
      mCond.wait(lock);
      if (mConnected == false)
	return false;
    }
    eventPtr = mQueue.front();
    mQueue.pop();
    return true;
  }

private:
  queue<EventPtr>   mQueue;
  boost::mutex      mMutex;
  boost::condition  mCond;
  bool              mConnected;
};



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
  
  if (argc == 1)
    Usage::DumpAndExit(usage);

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
    else if (!strncmp(argv[i], "--reactors=", 11))
      reactorCount = atoi(&argv[i][11]);
    else if (inputFile == 0)
      inputFile = argv[i];
    else
      Usage::DumpAndExit(usage);
  }

  if (inputFile == 0)
    Usage::DumpAndExit(usage);

  memset(&addr, 0, sizeof(struct sockaddr_in));
  {
    struct hostent *he = gethostbyname(host);
    if (he == 0) {
      herror("gethostbyname()");
      exit(1);
    }
    memcpy(&addr.sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
  }
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);

  System::Initialize(argv[0]);
  ReactorFactory::Initialize(1);

  comm = new Comm();

  ResponseHandler *respHandler = new ResponseHandler();

  if ((error = comm->Connect(addr, timeout, respHandler)) != Error::OK) {
    LOG_VA_ERROR("Comm::Connect error - %s", Error::GetText(error));
    exit(1);
  }

  if (!respHandler->WaitForConnection())
    exit(1);

  int outstanding = 0;
  int maxOutstanding = 50;
  string line;
  ifstream myfile(inputFile);
  const char *str;

  if (myfile.is_open()) {
    while (!myfile.eof() ) {
      getline (myfile,line);
      if (line.length() > 0) {
	hbuilder.AssignUniqueId();
	CommBufPtr cbufPtr( new CommBuf(hbuilder, Serialization::EncodedLengthString(line)) );
	cbufPtr->AppendString(line);
	int retries = 0;
	while ((error = comm->SendRequest(addr, cbufPtr, respHandler)) != Error::OK) {
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
	  eventPtr.reset();
	  outstanding--;
	}
      }
    }
    myfile.close();
  }
  else {
    LOG_VA_ERROR("Unable to open file '%s' : %s", inputFile, strerror(errno));
    return 0;
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
    eventPtr.reset();
    outstanding--;
  }

  delete comm;
  return 0;
}


