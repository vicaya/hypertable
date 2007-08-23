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
 * This method adds a connection to the connection manager if it is not already added.
 * It first allocates a ConnectionStateT structure for the connection and initializes
 * it to the non-connected state.  It then attempts to establish the connection.
 *
 * @param addr         The address to connect to
 * @param timeout      The timeout passed down into AsyncComm, also used as the retry timeout
 * @param serviceName  The name of the service we're connnecting to (used for better log messages)
 */
void ConnectionManager::Add(struct sockaddr_in &addr, time_t timeout, const char *serviceName) {
  boost::mutex::scoped_lock lock(mImpl->mutex);
  SockAddrMapT<ConnectionStateT *> iter;
  ConnectionStateT *connState;
  int error;

  if (mImpl->connMap.find(addr) != mImpl->connMap.end())
    return;

  connState = new ConnectionStateT;
  connState->connected = false;
  connState->addr = addr;
  connState->timeout = timeout;
  connState->serviceName = (serviceName) ? serviceName : "";
  boost::xtime_get(&connState->nextRetry, boost::TIME_UTC);

  mImpl->connMap[addr] = connState;

  {
    boost::mutex::scoped_lock connLock(connState->mutex);
    SendConnectRequest(connState);
  }
}



/**
 * This is the AsyncComm dispatch handler method.  It gets called for each connection
 * related event (establishment, disconnect, etc.) for each connection.  For connect
 * events, the connection's connected flag is set to true and it's condition variable
 * is signaled.  For all other events (e.g. disconnect or error), the connection's
 * connected flag is set to false and a retry is scheduled.
 *
 * @param eventPtr shared pointer to event object
 */
void ConnectionManager::handle(EventPtr &eventPtr) {
  boost::mutex::scoped_lock lock(mImpl->mutex);
  SockAddrMapT<ConnectionStateT *>::iterator iter = mImpl->connMap.find(eventPtr->addr);

  if (iter != mImpl->connMap.end()) {
    boost::mutex::scoped_lock connLock((*iter).second->mutex);
    ConnectionStateT *connState = (*iter).second;

    if (eventPtr->type == Event::CONNECTION_ESTABLISHED) {
      connState->connected = true;
      connState->cond.notify_all();
    }
    else {
      if (!mImpl->quietMode) {
	LOG_VA_INFO("%s", eventPtr->toString().c_str());
      }
      connState->connected = false;
      // this logic could proably be smarter.  For example, if the last
      // connection attempt was a long time ago, then schedule immediately
      // otherwise, if this event is the result of an immediately prior connect
      // attempt, then do the following
      boost::xtime_get(&connState->nextRetry, boost::TIME_UTC);
      connState->nextRetry.sec += connState->timeout;
      // add to retry heap
      mImpl->retryQueue.push(connState);
      mImpl->retryCond.notify_one();
    }
  }
  else {
    LOG_VA_ERROR("Unable to find connection for %s:%d in map.", inet_ntoa(eventPtr->addr.sin_addr), ntohs(eventPtr->addr.sin_port));
  }
}



/**
 * This is the boost::thread run method.
 */
void ConnectionManager::operator()() {
  boost::mutex::scoped_lock lock(mImpl->mutex);
  ConnectionStateT *connState;

  while (true) {

    while (mImpl->retryQueue.empty())
      mImpl->retryCond.wait(lock);

    connState = mImpl->retryQueue.top();

    if (!connState->connected) {
      boost::mutex::scoped_lock connLock(connState->mutex);
      boost::xtime now;
      boost::xtime_get(&now, boost::TIME_UTC);

      if (xtime_cmp(connState->nextRetry, now) <= 0) {
	mImpl->retryQueue.pop();
	if (!mImpl->quietMode) {
	  LOG_VA_INFO("Attempting to re-establish connection to %s at %s:%d...",
		      connState->serviceName.c_str(), inet_ntoa(connState->addr.sin_addr), ntohs(connState->addr.sin_port));
	}
	SendConnectRequest(connState);
      }
      else
	mImpl->retryCond.timed_wait(lock, connState->nextRetry);
    }
    else
      mImpl->retryQueue.pop();
  }
}


bool ConnectionManager::WaitForConnection(struct sockaddr_in &addr, long maxWaitSecs) {
  ConnectionStateT *connState;

  {
    boost::mutex::scoped_lock lock(mImpl->mutex);
    SockAddrMapT<ConnectionStateT *>::iterator iter = mImpl->connMap.find(addr);
    assert(iter != mImpl->connMap.end());
    connState = (*iter).second;
  }

  {
    boost::mutex::scoped_lock connLock(connState->mutex);
    boost::xtime dropTime, now;

    boost::xtime_get(&dropTime, boost::TIME_UTC);
    dropTime.sec += maxWaitSecs;

    while (!connState->connected) {
      connState->cond.timed_wait(connLock, dropTime);
      boost::xtime_get(&now, boost::TIME_UTC);
      if (xtime_cmp(now, dropTime) >= 0)
	return false;
    }
  }

  return true;
}



/**
 * Attempts to establish a connection for the given ConnectionState object.  If a failure
 * occurs, it prints an error message and then schedules a retry by updating the
 * nextRetry member of the connState object and pushing it onto the retry heap
 *
 * @param connState The connection state record
 */
void ConnectionManager::SendConnectRequest(ConnectionStateT *connState) {
  int error;

  error = mImpl->comm->Connect(connState->addr, connState->timeout, this);

  if (error == Error::COMM_ALREADY_CONNECTED) {
    connState->connected = true;
    connState->cond.notify_all();
  }
  else if (error != Error::OK) {
    if (connState->serviceName != "") {
      LOG_VA_ERROR("Connection attempt to %s at %s:%d failed - %s.  Will retry again in %d seconds...", 
		   connState->serviceName.c_str(), inet_ntoa(connState->addr.sin_addr), ntohs(connState->addr.sin_port),
		   Error::GetText(error), connState->timeout);
    }
    else {
      LOG_VA_ERROR("Connection attempt to service at %s:%d failed - %s.  Will retry again in %d seconds...",
		   inet_ntoa(connState->addr.sin_addr), ntohs(connState->addr.sin_port),
		   Error::GetText(error), connState->timeout);
    }

    // reschedule
    boost::xtime_get(&connState->nextRetry, boost::TIME_UTC);
    connState->nextRetry.sec += connState->timeout;

    // add to retry heap
    mImpl->retryQueue.push(connState);
    mImpl->retryCond.notify_one();
  }

}



