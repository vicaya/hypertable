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
void ConnectionManager::Callback::handle(Event &event) {
  boost::mutex::scoped_lock lock(mMutex);

  //event.Display();

  if (event.type == Event::CONNECTION_ESTABLISHED) {
    mConnected = true;
    mCond.notify_all();
  }
  else if (event.type == Event::DISCONNECT) {
    mConnected = false;
    LOG_VA_INFO("%s", event.toString().c_str());
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


bool ConnectionManager::Callback::WaitForEvent(long maxWaitSecs) {
  boost::xtime xt;
  boost::mutex::scoped_lock lock(mMutex);

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


