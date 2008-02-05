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

#include <cstdlib>
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

using namespace Hypertable;
using namespace std;


/**
 */
void ConnectionManager::add(struct sockaddr_in &addr, time_t timeout, const char *serviceName, DispatchHandlerPtr &handlerPtr) {
  boost::mutex::scoped_lock lock(m_impl->mutex);
  SockAddrMapT<ConnectionStatePtr> iter;
  ConnectionState *connState;

  if (m_impl->connMap.find(addr) != m_impl->connMap.end())
    return;

  connState = new ConnectionState();
  connState->connected = false;
  connState->addr = addr;
  memset(&connState->localAddr, 0, sizeof(struct sockaddr_in));
  connState->timeout = timeout;
  connState->handlerPtr = handlerPtr;
  connState->serviceName = (serviceName) ? serviceName : "";
  boost::xtime_get(&connState->nextRetry, boost::TIME_UTC);

  m_impl->connMap[addr] = ConnectionStatePtr(connState);

  {
    boost::mutex::scoped_lock connLock(connState->mutex);
    send_connect_request(connState);
  }
}

/**
 *
 */
void ConnectionManager::add(struct sockaddr_in &addr, time_t timeout, const char *serviceName) {
  DispatchHandlerPtr nullDispatchHandler;
  add(addr, timeout, serviceName, nullDispatchHandler);
}



/**
 *
 */
void ConnectionManager::add(struct sockaddr_in &addr, struct sockaddr_in &localAddr, time_t timeout, const char *serviceName, DispatchHandlerPtr &handlerPtr) {
  boost::mutex::scoped_lock lock(m_impl->mutex);
  SockAddrMapT<ConnectionStatePtr> iter;
  ConnectionState *connState;

  if (m_impl->connMap.find(addr) != m_impl->connMap.end())
    return;

  connState = new ConnectionState();
  connState->connected = false;
  connState->addr = addr;
  connState->localAddr = localAddr;
  connState->timeout = timeout;
  connState->handlerPtr = handlerPtr;
  connState->serviceName = (serviceName) ? serviceName : "";
  boost::xtime_get(&connState->nextRetry, boost::TIME_UTC);

  m_impl->connMap[addr] = ConnectionStatePtr(connState);

  {
    boost::mutex::scoped_lock connLock(connState->mutex);
    send_connect_request(connState);
  }
}


/**
 *
 */
void ConnectionManager::add(struct sockaddr_in &addr, struct sockaddr_in &localAddr, time_t timeout, const char *serviceName) {
  DispatchHandlerPtr nullDispatchHandler;
  add(addr, localAddr, timeout, serviceName, nullDispatchHandler);
}




bool ConnectionManager::wait_for_connection(struct sockaddr_in &addr, long maxWaitSecs) {
  ConnectionStatePtr connStatePtr;

  {
    boost::mutex::scoped_lock lock(m_impl->mutex);
    SockAddrMapT<ConnectionStatePtr>::iterator iter = m_impl->connMap.find(addr);
    if (iter == m_impl->connMap.end())
      return false;
    connStatePtr = (*iter).second;
  }

  {
    boost::mutex::scoped_lock connLock(connStatePtr->mutex);
    boost::xtime dropTime, now;

    boost::xtime_get(&dropTime, boost::TIME_UTC);
    dropTime.sec += maxWaitSecs;

    while (!connStatePtr->connected) {
      connStatePtr->cond.timed_wait(connLock, dropTime);
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
void ConnectionManager::send_connect_request(ConnectionState *connState) {
  int error;
  DispatchHandlerPtr handlerPtr(this);

  if (connState->localAddr.sin_port != 0)
    error = m_impl->comm->connect(connState->addr, connState->localAddr, handlerPtr);
  else
    error = m_impl->comm->connect(connState->addr, handlerPtr);

  if (error == Error::COMM_ALREADY_CONNECTED) {
    connState->connected = true;
    connState->cond.notify_all();
  }
  else if (error != Error::OK) {
    if (connState->serviceName != "") {
      HT_ERRORF("Connection attempt to %s at %s:%d failed - %s.  Will retry again in %d seconds...", 
		   connState->serviceName.c_str(), inet_ntoa(connState->addr.sin_addr), ntohs(connState->addr.sin_port),
		   Error::get_text(error), connState->timeout);
    }
    else {
      HT_ERRORF("Connection attempt to service at %s:%d failed - %s.  Will retry again in %d seconds...",
		   inet_ntoa(connState->addr.sin_addr), ntohs(connState->addr.sin_port),
		   Error::get_text(error), connState->timeout);
    }

    // reschedule
    boost::xtime_get(&connState->nextRetry, boost::TIME_UTC);
    int32_t sec_addition;
    if (rand() % 2)
      sec_addition = connState->timeout + (rand() % 2);
    else
      sec_addition = connState->timeout - (rand() % 2);
    if (sec_addition < 1)
      sec_addition = 1;
    connState->nextRetry.sec += sec_addition;
    connState->nextRetry.nsec = ((int64_t)rand() << 32) | rand();

    // add to retry heap
    m_impl->retryQueue.push(connState);
    m_impl->retryCond.notify_one();
  }

}



/**
 *
 */
int ConnectionManager::remove(struct sockaddr_in &addr) {
  bool doClose = false;
  int error = Error::OK;

  {
    boost::mutex::scoped_lock lock(m_impl->mutex);
    SockAddrMapT<ConnectionStatePtr>::iterator iter = m_impl->connMap.find(addr);

    if (iter != m_impl->connMap.end()) {
      boost::mutex::scoped_lock connLock((*iter).second->mutex);
      if ((*iter).second->connected)
	doClose = true;
      else
	(*iter).second->connected = true;  // will prevent further connection attempts
      m_impl->connMap.erase(iter);
    }
  }

  if (doClose)
    error = m_impl->comm->close_socket(addr);

  return error;
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
  boost::mutex::scoped_lock lock(m_impl->mutex);
  SockAddrMapT<ConnectionStatePtr>::iterator iter = m_impl->connMap.find(eventPtr->addr);

  if (iter != m_impl->connMap.end()) {
    boost::mutex::scoped_lock connLock((*iter).second->mutex);
    ConnectionState *connState = (*iter).second.get();

    if (eventPtr->type == Event::CONNECTION_ESTABLISHED) {
      connState->connected = true;
      connState->cond.notify_all();
    }
    else if (eventPtr->type == Event::ERROR || eventPtr->type == Event::DISCONNECT) {
      if (!m_impl->quietMode) {
	HT_INFOF("%s; Problem connecting to %s, will retry in %d seconds...",
		    eventPtr->toString().c_str(), connState->serviceName.c_str(), connState->timeout);
      }
      connState->connected = false;
      // this logic could proably be smarter.  For example, if the last
      // connection attempt was a long time ago, then schedule immediately
      // otherwise, if this event is the result of an immediately prior connect
      // attempt, then do the following
      boost::xtime_get(&connState->nextRetry, boost::TIME_UTC);
      connState->nextRetry.sec += connState->timeout;
      // add to retry heap
      m_impl->retryQueue.push(connState);
      m_impl->retryCond.notify_one();
    }
    
    // Chain event to application supplied handler
    if (connState->handlerPtr)
      connState->handlerPtr->handle(eventPtr);
  }
  else {
    HT_WARNF("Unable to find connection for %s:%d in map.", inet_ntoa(eventPtr->addr.sin_addr), ntohs(eventPtr->addr.sin_port));
  }
}



/**
 * This is the boost::thread run method.
 */
void ConnectionManager::operator()() {
  boost::mutex::scoped_lock lock(m_impl->mutex);
  ConnectionStatePtr connStatePtr;

  while (true) {

    while (m_impl->retryQueue.empty())
      m_impl->retryCond.wait(lock);

    connStatePtr = m_impl->retryQueue.top();

    if (!connStatePtr->connected) {
      {
	boost::mutex::scoped_lock connLock(connStatePtr->mutex);
	boost::xtime now;
	boost::xtime_get(&now, boost::TIME_UTC);

	if (xtime_cmp(connStatePtr->nextRetry, now) <= 0) {
	  m_impl->retryQueue.pop();
	  /**
	     if (!m_impl->quietMode) {
	     HT_INFOF("Attempting to re-establish connection to %s at %s:%d...",
	     connStatePtr->serviceName.c_str(), inet_ntoa(connStatePtr->addr.sin_addr), ntohs(connStatePtr->addr.sin_port));
	     }
	  */
	  send_connect_request(connStatePtr.get());
	  continue;
	}
      }
      m_impl->retryCond.timed_wait(lock, connStatePtr->nextRetry);
    }
    else
      m_impl->retryQueue.pop();
  }
}


