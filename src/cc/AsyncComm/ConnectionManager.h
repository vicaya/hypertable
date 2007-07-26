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

	if (mHandler->WaitForConnection(maxWaitSecs-elapsed))
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
      virtual void handle(EventPtr &event);
      void SendConnectRequest();
      bool WaitForConnection(long maxWaitSecs);
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
