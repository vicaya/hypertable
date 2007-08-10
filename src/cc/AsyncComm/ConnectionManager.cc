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

#include <boost/thread/xtime.hpp>

extern "C" {
#include <poll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
}

#include "Common/Error.h"
#include "Common/Logger.h"

#include "AsyncComm/Comm.h"

#include "ConnectionManager.h"

using namespace hypertable;
using namespace std;


/**
 *
 */
void ConnectionManager::operator()() {
  struct timeval tval;
  long elapsed, sendtime = 0;

  while (true) {

    gettimeofday(&tval, 0);
    elapsed = tval.tv_sec - sendtime;

    if (elapsed < RETRY_INTERVAL) {
      cerr << mWaitMessage << " ...";
      poll(0, 0, (RETRY_INTERVAL - elapsed)*1000);
      cerr << endl;
    }

    mHandler->SendConnectRequest();
    gettimeofday(&tval, 0);
    sendtime = tval.tv_sec;

    while (mHandler->WaitForEvent())
      ;

  }
  
}



/**
 *
 */
void ConnectionManager::Callback::handle(EventPtr &eventPtr) {
  boost::mutex::scoped_lock lock(mMutex);

  //event.Display();

  if (eventPtr->type == Event::CONNECTION_ESTABLISHED) {
    mConnected = true;
    mCond.notify_all();
  }
  else if (eventPtr->type == Event::DISCONNECT) {
    mConnected = false;
    LOG_VA_INFO("%s", eventPtr->toString().c_str());
    mCond.notify_all();
  }

}


void ConnectionManager::Callback::SendConnectRequest() {

  int error = mComm->Connect(mAddr, mTimeout, this);

  if (error == Error::COMM_ALREADY_CONNECTED) {
    mConnected = true;
    mCond.notify_all();
  }
  else if (error != Error::OK) {
    LOG_VA_ERROR("Comm::Connect to %s:%d failed - %s", inet_ntoa(mAddr.sin_addr), ntohs(mAddr.sin_port), Error::GetText(error));
  }
}


bool ConnectionManager::Callback::WaitForConnection(long maxWaitSecs) {
  boost::xtime xt;
  boost::mutex::scoped_lock lock(mMutex);

  if (mConnected)
    return true;

  boost::xtime_get(&xt, boost::TIME_UTC);
  xt.sec += maxWaitSecs;

  mCond.timed_wait(lock, xt);

  return mConnected;
}



bool ConnectionManager::Callback::WaitForEvent() {
  boost::mutex::scoped_lock lock(mMutex);

  mCond.wait(lock);

  return mConnected;
}


