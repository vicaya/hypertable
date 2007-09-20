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
#include <fstream>
#include <queue>

extern "C" {
#include <poll.h>
}

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "Common/Error.h"
#include "Common/InetAddr.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/DispatchHandler.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/HeaderBuilder.h"
#include "AsyncComm/Serialization.h"

#include "CommTestDatagramThreadFunction.h"

using namespace hypertable;
using namespace std;

namespace {


  /**
   * This is the dispatch handler that gets installed as the default
   * dispatch handler for the receive port.  It queues up the responses
   * which can be fetched by the application via a call to GetResponse()
   */
  class ResponseHandler : public DispatchHandler {

  public:

    ResponseHandler() : mQueue(), mMutex(), mCond() { return; }
  
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

  private:
    queue<EventPtr>   mQueue;
    boost::mutex      mMutex;
    boost::condition  mCond;
  };

}


/**
 *
 */
void CommTestDatagramThreadFunction::operator()() {
  HeaderBuilder hbuilder(Header::PROTOCOL_NONE, rand());
  int error;
  EventPtr eventPtr;
  int outstanding = 0;
  int maxOutstanding = 50;
  string line;
  ifstream infile(mInputFile);
  ofstream outfile(mOutputFile);
  const char *str;
  uint64_t gid64 = (uint64_t)this;
  uint32_t gid = (uint32_t)(gid64 & 0x00000000FFFFFFFFLL);
  ResponseHandler respHandler;
  int nsent = 0;
  struct sockaddr_in localAddr;

  InetAddr::Initialize(&localAddr, INADDR_ANY, mPort);

  if (error = mComm->CreateDatagramReceiveSocket(localAddr, &respHandler)) {
    LOG_VA_ERROR("Problem opening datagram receive port %d - %s", mPort, Error::GetText(error));
    return;
  }

  if (infile.is_open()) {
    while (!infile.eof() && nsent < MAX_MESSAGES) {
      getline (infile,line);
      if (line.length() > 0) {
	hbuilder.AssignUniqueId();
	CommBufPtr cbufPtr( new CommBuf(hbuilder, Serialization::EncodedLengthString(line)) );
	cbufPtr->AppendString(line);
	if ((error = mComm->SendDatagram(mAddr, localAddr, cbufPtr)) != Error::OK) {
	  LOG_VA_ERROR("Problem sending datagram - %s", Error::GetText(error));
	  return;
	}
	outstanding++;

	if (outstanding  > maxOutstanding) {
	  if (!respHandler.GetResponse(eventPtr))
	    break;
	  if (!Serialization::DecodeString(&eventPtr->message, &eventPtr->messageLen, &str))
	    outfile << "ERROR: deserialization problem." << endl;
	  else {
	    if (*str != 0)
	      outfile << str << endl;
	    else
	      outfile << "[NULL]" << endl;
	  }
	  outstanding--;
	}
      }
      nsent++;
    }
    infile.close();
  }
  else {
    LOG_VA_ERROR("Unable to open file '%s' : %s", mInputFile, strerror(errno));
    return;
  }

  while (outstanding > 0 && respHandler.GetResponse(eventPtr)) {
    if (!Serialization::DecodeString(&eventPtr->message, &eventPtr->messageLen, &str))
      outfile << "ERROR: deserialization problem." << endl;
    else {
      if (*str != 0)
	outfile << str << endl;
      else
	outfile << "[NULL]" << endl;
    }
    //cout << "out = " << outstanding << endl;
    outstanding--;
  }
}
