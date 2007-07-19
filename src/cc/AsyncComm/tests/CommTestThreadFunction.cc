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
#include <fstream>
#include <queue>

extern "C" {
#include <poll.h>
}

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "Common/Error.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/CallbackHandler.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/MessageBuilderSimple.h"

#include "CommTestThreadFunction.h"

using namespace hypertable;
using namespace std;

namespace {

  /**
   *
   */
  class ResponseHandler : public CallbackHandler {

  public:

    ResponseHandler() : mQueue(), mMutex(), mCond(), mConnected(true) { return; }
  
    virtual void handle(Event &event) {
      boost::mutex::scoped_lock lock(mMutex);
      if (event.type == Event::MESSAGE) {
	Event *newEvent = new Event(event);
	mQueue.push(newEvent);
	event.header = 0;
	mCond.notify_one();
      }
      else {
	event.Display();
	mConnected = false;
	mCond.notify_one();
      }
    }

    Event *GetResponse() {
      boost::mutex::scoped_lock lock(mMutex);
      while (mQueue.empty()) {
	mCond.wait(lock);
	if (mConnected == false)
	  return 0;
      }
      Event *event = mQueue.front();
      mQueue.pop();
      return event;
    }

  private:
    queue<Event *>    mQueue;
    boost::mutex      mMutex;
    boost::condition  mCond;
    bool              mConnected;
  };
}


/**
 *
 */
void CommTestThreadFunction::operator()() {
  MessageBuilderSimple *msgBuilder = new MessageBuilderSimple();
  int error;
  Event *event;
  CommBuf *cbuf;
  int outstanding = 0;
  int maxOutstanding = 50;
  string line;
  ifstream infile(mInputFile);
  ofstream outfile(mOutputFile);
  const char *str;

  ResponseHandler *respHandler = new ResponseHandler();

  if (infile.is_open()) {
    while (!infile.eof() ) {
      msgBuilder->Reset(Message::PROTOCOL_NONE);
      getline (infile,line);
      if (line.length() > 0) {
	cbuf = new CommBuf(msgBuilder->HeaderLength() + CommBuf::EncodedLength(line));
	cbuf->PrependString(line);
	msgBuilder->Encapsulate(cbuf);
	int retries = 0;
	while ((error = mComm->SendRequest(mAddr, cbuf, respHandler)) != Error::OK) {
	  if (error == Error::COMM_NOT_CONNECTED) {
	    if (retries == 5) {
	      LOG_ERROR("Connection timeout.");
	      return;
	    }
	    poll(0, 0, 1000);
	    retries++;
	  }
	  else {
	    LOG_VA_ERROR("CommEngine::SendMessage returned '%s'", Error::GetText(error));
	    return;
	  }
	}
	outstanding++;

	if (outstanding  > maxOutstanding) {
	  if ((event = respHandler->GetResponse()) == 0)
	    break;
	  CommBuf::DecodeString(event->message, event->messageLen, &str);
	  if (str != 0)
	    outfile << str << endl;
	  else
	    outfile << "[NULL]" << endl;
	  delete event;
	  outstanding--;
	}
      }
    }
    infile.close();
  }
  else {
    LOG_VA_ERROR("Unable to open file '%s' : %s", mInputFile, strerror(errno));
    return;
  }

  while (outstanding > 0 && (event = respHandler->GetResponse()) != 0) {
    CommBuf::DecodeString(event->message, event->messageLen, &str);
    if (str != 0)
      outfile << str << endl;
    else
      outfile << "[NULL]" << endl;
    //cout << "out = " << outstanding << endl;
    delete event;
    outstanding--;
  }
}
