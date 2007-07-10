/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef HYPERTABLE_CONNECTIONMANAGER_H
#define HYPERTABLE_CONNECTIONMANAGER_H

#include <string>

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

extern "C" {
#include <time.h>
#include <sys/time.h>
}

#include "CallbackHandler.h"

namespace hypertable {

  class Comm;
  class Event;

  class ConnectionManager {

  public:

    static const int RETRY_INTERVAL = 10;

    ConnectionManager(std::string &waitMessage) : mWaitMessage(waitMessage) { return; }

    ConnectionManager(const char *waitMessage) : mWaitMessage(waitMessage) { return; }

    void Initiate(Comm *comm, struct sockaddr_in &addr, time_t timeout) {
      mHandler = new Callback(comm, addr, timeout);
      mThread = new boost::thread(*this);
      return;
    }

    bool WaitForConnection(long maxWaitSecs) {
      struct timeval tval;
      long elapsed = 0;
      long starttime;

      gettimeofday(&tval, 0);
      starttime = tval.tv_sec;

      while (elapsed < maxWaitSecs) {

	if (mHandler->WaitForEvent(maxWaitSecs-elapsed))
	  return true;

	gettimeofday(&tval, 0);
	elapsed = tval.tv_sec - starttime;
      }
      return false;
    }

    void operator()();

    class Callback : public CallbackHandler {
    public:
      Callback(Comm *comm, struct sockaddr_in &addr, time_t timeout) 
	: mComm(comm), mAddr(addr), mTimeout(timeout), mConnected(false), mMutex(), mCond() { return; }
      virtual ~Callback() { return; }
      virtual void handle(Event &event);
      void SendConnectRequest();
      bool WaitForEvent(long maxWaitSecs);
      bool WaitForEvent();

    private:
      Comm               *mComm;
      struct sockaddr_in  mAddr;
      time_t              mTimeout;
      bool                mConnected;
      boost::mutex        mMutex;
      boost::condition    mCond;
    };

  private:
    Callback       *mHandler;
    boost::thread  *mThread;
    std::string     mWaitMessage;
  };

}


#endif // HYPERTABLE_CONNECTIONMANAGER_H
