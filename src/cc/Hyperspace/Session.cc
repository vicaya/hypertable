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

#include <cassert>

extern "C" {
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
}

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/Serialization.h"

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/Properties.h"

#include "ClientHandleState.h"
#include "Master.h"
#include "Protocol.h"
#include "Session.h"

using namespace hypertable;
using namespace Hyperspace;


/**
 * 
 */
Session::Session(Comm *comm, PropertiesPtr &propsPtr, SessionCallback *callback) : m_comm(comm), m_verbose(false), m_state(STATE_JEOPARDY), m_session_callback(callback) {
  uint16_t masterPort;
  const char *masterHost;

  m_verbose = propsPtr->getPropertyBool("verbose", false);
  masterHost = propsPtr->getProperty("Hyperspace.Master.host", "localhost");
  masterPort = (uint16_t)propsPtr->getPropertyInt("Hyperspace.Master.port", Master::DEFAULT_MASTER_PORT);
  m_grace_period = (uint32_t)propsPtr->getPropertyInt("Hyperspace.GracePeriod", Master::DEFAULT_GRACEPERIOD);
  m_lease_interval = (uint32_t)propsPtr->getPropertyInt("Hyperspace.Lease.Interval", Master::DEFAULT_LEASE_INTERVAL);
  m_timeout = m_lease_interval * 2;

  if (!InetAddr::initialize(&m_master_addr, masterHost, masterPort))
    exit(1);

  boost::xtime_get(&m_expire_time, boost::TIME_UTC);
  m_expire_time.sec += m_grace_period;

  if (m_verbose) {
    cout << "Hyperspace.GracePeriod=" << m_grace_period << endl;
  }

  m_keepalive_handler_ptr = new ClientKeepaliveHandler(comm, propsPtr, this);
}

Session::~Session() {
}


int Session::open(ClientHandleStatePtr &handleStatePtr, CommBufPtr &cbufPtr, uint64_t *handlep) {
  DispatchHandlerSynchronizer syncHandler;
  hypertable::EventPtr eventPtr;

  handleStatePtr->handle = 0;
  handleStatePtr->sequencer = 0;
  handleStatePtr->lockStatus = 0;

  if ((handleStatePtr->openFlags & OPEN_FLAG_LOCK_SHARED) == OPEN_FLAG_LOCK_SHARED)
    handleStatePtr->lockMode = LOCK_MODE_SHARED;
  else if ((handleStatePtr->openFlags & OPEN_FLAG_LOCK_EXCLUSIVE) == OPEN_FLAG_LOCK_EXCLUSIVE)
    handleStatePtr->lockMode = LOCK_MODE_EXCLUSIVE;
  else
    handleStatePtr->lockMode = 0;

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;
  
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      error = (int)Protocol::response_code(eventPtr.get());
      LOG_VA_ERROR("Hyperspace 'open' error, name=%s flags=0x%x events=0x%x : %s",
		   handleStatePtr->normalName.c_str(), handleStatePtr->openFlags,
		   handleStatePtr->eventMask, Error::get_text(error));
      if (m_verbose)
	LOG_VA_ERROR("%s", Protocol::string_format_message(eventPtr.get()).c_str());
    }
    else {
      uint8_t *ptr = eventPtr->message + 4;
      size_t remaining = eventPtr->messageLen - 4;
      uint8_t cbyte;
      if (!Serialization::decode_long(&ptr, &remaining, handlep))
	return Error::RESPONSE_TRUNCATED;
      if (!Serialization::decode_byte(&ptr, &remaining, &cbyte))
	return Error::RESPONSE_TRUNCATED;
      if (!Serialization::decode_long(&ptr, &remaining, &handleStatePtr->lockGeneration))
	return Error::RESPONSE_TRUNCATED;
      /** if (createdp) *createdp = cbyte ? true : false; **/
      handleStatePtr->handle = *handlep;
      m_keepalive_handler_ptr->register_handle(handleStatePtr);
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}



/**
 *
 */
int Session::open(std::string name, uint32_t flags, HandleCallbackPtr &callbackPtr, uint64_t *handlep) {
  ClientHandleStatePtr handleStatePtr( new ClientHandleState() );
  std::vector<AttributeT> emptyAttrs;

  handleStatePtr->openFlags = flags;
  handleStatePtr->eventMask = (callbackPtr) ? callbackPtr->get_event_mask() : 0;
  handleStatePtr->callbackPtr = callbackPtr;
  normalize_name(name, handleStatePtr->normalName);

  CommBufPtr cbufPtr( Protocol::create_open_request(handleStatePtr->normalName, flags, callbackPtr, emptyAttrs) );

  return open(handleStatePtr, cbufPtr, handlep);

}



/**
 *
 */
int Session::create(std::string name, uint32_t flags, HandleCallbackPtr &callbackPtr, std::vector<AttributeT> &initAttrs, uint64_t *handlep) {
  ClientHandleStatePtr handleStatePtr( new ClientHandleState() );

  handleStatePtr->openFlags = flags | OPEN_FLAG_CREATE | OPEN_FLAG_EXCL;
  handleStatePtr->eventMask = (callbackPtr) ? callbackPtr->get_event_mask() : 0;
  handleStatePtr->callbackPtr = callbackPtr;
  normalize_name(name, handleStatePtr->normalName);

  CommBufPtr cbufPtr( Protocol::create_open_request(handleStatePtr->normalName, handleStatePtr->openFlags, callbackPtr, initAttrs) );

  return open(handleStatePtr, cbufPtr, handlep);
}



/**
 *
 */
int Session::close(uint64_t handle) {
  DispatchHandlerSynchronizer syncHandler;
  hypertable::EventPtr eventPtr;
  CommBufPtr cbufPtr( Protocol::create_close_request(handle) );

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      error = (int)Protocol::response_code(eventPtr.get());
      LOG_VA_ERROR("Hyperspace 'close' error : %s", Error::get_text(error));
      if (m_verbose)
	LOG_VA_ERROR("%s", Protocol::string_format_message(eventPtr.get()).c_str());
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}



/**
 * Submit 'mkdir' request
 */
int Session::mkdir(std::string name) {
  DispatchHandlerSynchronizer syncHandler;
  hypertable::EventPtr eventPtr;
  std::string normalName;

  normalize_name(name, normalName);

  CommBufPtr cbufPtr( Protocol::create_mkdir_request(normalName) );

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;
  
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      error = (int)Protocol::response_code(eventPtr.get());
      LOG_VA_ERROR("Hyperspace 'mkdir' error, name=%s : %s", normalName.c_str(), Error::get_text(error));
      if (m_verbose)
	LOG_VA_ERROR("%s", Protocol::string_format_message(eventPtr.get()).c_str());
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}


int Session::unlink(std::string name) {
  DispatchHandlerSynchronizer syncHandler;
  hypertable::EventPtr eventPtr;
  std::string normalName;

  normalize_name(name, normalName);

  CommBufPtr cbufPtr( Protocol::create_delete_request(normalName) );

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;
  
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      error = (int)Protocol::response_code(eventPtr.get());
      LOG_VA_ERROR("Hyperspace 'delete' error, name=%s : %s", normalName.c_str(), Error::get_text(error));
      if (m_verbose)
	LOG_VA_ERROR("%s", Protocol::string_format_message(eventPtr.get()).c_str());
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}


int Session::exists(std::string name, bool *existsp) {
  DispatchHandlerSynchronizer syncHandler;
  hypertable::EventPtr eventPtr;
  std::string normalName;

  normalize_name(name, normalName);

  CommBufPtr cbufPtr( Protocol::create_exists_request(normalName) );

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;
  
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      error = (int)Protocol::response_code(eventPtr.get());
      LOG_VA_ERROR("Hyperspace 'exists' error, name=%s : %s", normalName.c_str(), Error::get_text(error));
      if (m_verbose)
	LOG_VA_ERROR("%s", Protocol::string_format_message(eventPtr.get()).c_str());
    }
    else {
      uint8_t *ptr = eventPtr->message + 4;
      size_t remaining = eventPtr->messageLen - 4;
      uint8_t bval;
      if (!Serialization::decode_byte(&ptr, &remaining, &bval))
	assert(!"problem decoding return packet");
      *existsp = (bval == 0) ? false : true;
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}



/**
 *  Blocking 'attrset' request
 */
int Session::attr_set(uint64_t handle, std::string name, const void *value, size_t valueLen) {
  DispatchHandlerSynchronizer syncHandler;
  hypertable::EventPtr eventPtr;
  CommBufPtr cbufPtr( Protocol::create_attr_set_request(handle, name, value, valueLen) );

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      error = (int)Protocol::response_code(eventPtr.get());
      LOG_VA_ERROR("Hyperspace 'attrset' error, name=%s : %s", name.c_str(), Error::get_text(error));
      if (m_verbose)
	LOG_VA_ERROR("%s", Protocol::string_format_message(eventPtr.get()).c_str());
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}


/**
 *
 */
int Session::attr_get(uint64_t handle, std::string name, DynamicBuffer &value) {
  DispatchHandlerSynchronizer syncHandler;
  hypertable::EventPtr eventPtr;
  CommBufPtr cbufPtr( Protocol::create_attr_get_request(handle, name) );

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      error = (int)Protocol::response_code(eventPtr.get());
      LOG_VA_ERROR("Hyperspace 'attrget' error, name=%s : %s", name.c_str(), Error::get_text(error));
      if (m_verbose)
	LOG_VA_ERROR("%s", Protocol::string_format_message(eventPtr.get()).c_str());
    }
    else {
      uint8_t *attrValue;
      int32_t attrValueLen;
      uint8_t *ptr = eventPtr->message + 4;
      size_t remaining = eventPtr->messageLen - 4;
      if (!Serialization::decode_byte_array(&ptr, &remaining, &attrValue, &attrValueLen)) {
	assert(!"problem decoding return packet");
      }
      value.clear();
      value.ensure(attrValueLen+1);
      value.addNoCheck(attrValue, attrValueLen);
      // nul-terminate to make caller's lives easier
      *value.ptr = 0;
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}


/**
 *
 */
int Session::attr_del(uint64_t handle, std::string name) {
  DispatchHandlerSynchronizer syncHandler;
  hypertable::EventPtr eventPtr;
  CommBufPtr cbufPtr( Protocol::create_attr_del_request(handle, name) );

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      error = (int)Protocol::response_code(eventPtr.get());
      LOG_VA_ERROR("Hyperspace 'attrdel' error, name=%s : %s", name.c_str(), Error::get_text(error));
      if (m_verbose)
	LOG_VA_ERROR("%s", Protocol::string_format_message(eventPtr.get()).c_str());
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}


int Session::readdir(uint64_t handle, std::vector<struct DirEntryT> &listing) {
  DispatchHandlerSynchronizer syncHandler;
  hypertable::EventPtr eventPtr;
  CommBufPtr cbufPtr( Protocol::create_readdir_request(handle) );

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      error = (int)Protocol::response_code(eventPtr.get());
      LOG_VA_ERROR("Hyperspace 'readdir' error : %s", Error::get_text(error));
      if (m_verbose)
	LOG_VA_ERROR("%s", Protocol::string_format_message(eventPtr.get()).c_str());
    }
    else {
      uint8_t *ptr = eventPtr->message + 4;
      size_t remaining = eventPtr->messageLen - 4;
      uint32_t entryCount;
      struct DirEntryT dentry;
      if (!Serialization::decode_int(&ptr, &remaining, &entryCount)) {
	LOG_ERROR("Problem decoding READDIR return packet");
	return Error::PROTOCOL_ERROR;
      }
      listing.clear();
      for (uint32_t i=0; i<entryCount; i++) {
	if (!Hyperspace::decode_range_dir_entry(&ptr, &remaining, dentry)) {
	  LOG_VA_ERROR("Problem decoding entry %d of READDIR return packet", i);
	  return Error::PROTOCOL_ERROR;
	}
	listing.push_back(dentry);
      }
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}



int Session::lock(uint64_t handle, uint32_t mode, struct LockSequencerT *sequencerp) {
  DispatchHandlerSynchronizer syncHandler;
  hypertable::EventPtr eventPtr;
  CommBufPtr cbufPtr( Protocol::create_lock_request(handle, mode, false) );
  ClientHandleStatePtr handleStatePtr;
  uint32_t status = 0;

  if (!m_keepalive_handler_ptr->get_handle_state(handle, handleStatePtr))
    return Error::HYPERSPACE_INVALID_HANDLE;

  if (handleStatePtr->lockStatus != 0)
    return Error::HYPERSPACE_ALREADY_LOCKED;

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  {
    boost::mutex::scoped_lock lock(handleStatePtr->mutex);
    sequencerp->mode = mode;
    sequencerp->name = handleStatePtr->normalName;
    handleStatePtr->sequencer = sequencerp;
  }

  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      error = (int)Protocol::response_code(eventPtr.get());
      LOG_VA_ERROR("Hyperspace 'lock' error, handle=%lld name='%s' : %s", handle, handleStatePtr->normalName.c_str(), Error::get_text(error));
      if (m_verbose)
	LOG_VA_ERROR("%s", Protocol::string_format_message(eventPtr.get()).c_str());
    }
    else {
      boost::mutex::scoped_lock lock(handleStatePtr->mutex);
      uint8_t *ptr = eventPtr->message + 4;
      size_t remaining = eventPtr->messageLen - 4;
      handleStatePtr->lockMode = mode;
      if (!Serialization::decode_int(&ptr, &remaining, &status))
	assert(!"problem decoding return packet");
      if (status == LOCK_STATUS_GRANTED) {
	if (!Serialization::decode_long(&ptr, &remaining, &sequencerp->generation))
	  assert(!"problem decoding return packet");
	handleStatePtr->lockGeneration = sequencerp->generation;
	handleStatePtr->lockStatus = LOCK_STATUS_GRANTED;
      }
      else {
	assert(status == LOCK_STATUS_PENDING);
	handleStatePtr->lockStatus = LOCK_STATUS_PENDING;
	while (handleStatePtr->lockStatus == LOCK_STATUS_PENDING)
	  handleStatePtr->cond.wait(lock);
	if (handleStatePtr->lockStatus == LOCK_STATUS_CANCELLED)
	  return Error::HYPERSPACE_REQUEST_CANCELLED;
	assert(handleStatePtr->lockStatus == LOCK_STATUS_GRANTED);
      }
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}



int Session::try_lock(uint64_t handle, uint32_t mode, uint32_t *statusp, struct LockSequencerT *sequencerp) {
  DispatchHandlerSynchronizer syncHandler;
  hypertable::EventPtr eventPtr;
  CommBufPtr cbufPtr( Protocol::create_lock_request(handle, mode, true) );
  ClientHandleStatePtr handleStatePtr;

  if (!m_keepalive_handler_ptr->get_handle_state(handle, handleStatePtr))
    return Error::HYPERSPACE_INVALID_HANDLE;

  if (handleStatePtr->lockStatus != 0)
    return Error::HYPERSPACE_ALREADY_LOCKED;

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      error = (int)Protocol::response_code(eventPtr.get());
      LOG_VA_ERROR("Hyperspace 'trylock' error, handle=%lld name='%s' : %s", handle, handleStatePtr->normalName.c_str(), Error::get_text(error));
      if (m_verbose)
	LOG_VA_ERROR("%s", Protocol::string_format_message(eventPtr.get()).c_str());
    }
    else {
      boost::mutex::scoped_lock lock(handleStatePtr->mutex);
      uint8_t *ptr = eventPtr->message + 4;
      size_t remaining = eventPtr->messageLen - 4;
      if (!Serialization::decode_int(&ptr, &remaining, statusp))
	assert(!"problem decoding return packet");
      if (*statusp == LOCK_STATUS_GRANTED) {
	if (!Serialization::decode_long(&ptr, &remaining, &sequencerp->generation))
	  assert(!"problem decoding return packet");
	sequencerp->mode = mode;
	sequencerp->name = handleStatePtr->normalName;
	handleStatePtr->lockMode = mode;
	handleStatePtr->lockStatus = LOCK_STATUS_GRANTED;
	handleStatePtr->lockGeneration = sequencerp->generation;
	handleStatePtr->sequencer = 0;
      }
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}


int Session::release(uint64_t handle) {
  DispatchHandlerSynchronizer syncHandler;
  hypertable::EventPtr eventPtr;
  CommBufPtr cbufPtr( Protocol::create_release_request(handle) );
  ClientHandleStatePtr handleStatePtr;

  if (!m_keepalive_handler_ptr->get_handle_state(handle, handleStatePtr))
    return Error::HYPERSPACE_INVALID_HANDLE;

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    boost::mutex::scoped_lock lock(handleStatePtr->mutex);
    if (!syncHandler.wait_for_reply(eventPtr)) {
      error = (int)Protocol::response_code(eventPtr.get());
      LOG_VA_ERROR("Hyperspace 'release' error : %s", Error::get_text(error));
      if (m_verbose)
	LOG_VA_ERROR("%s", Protocol::string_format_message(eventPtr.get()).c_str());
    }
    handleStatePtr->lockStatus = 0;
    handleStatePtr->cond.notify_all();
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}



int Session::get_sequencer(uint64_t handle, struct LockSequencerT *sequencerp) {
  ClientHandleStatePtr handleStatePtr;

  if (!m_keepalive_handler_ptr->get_handle_state(handle, handleStatePtr))
    return Error::HYPERSPACE_INVALID_HANDLE;

  if (handleStatePtr->lockGeneration == 0)
    return Error::HYPERSPACE_NOT_LOCKED;

  sequencerp->name = handleStatePtr->normalName;
  sequencerp->mode = handleStatePtr->lockMode;
  sequencerp->generation = handleStatePtr->lockGeneration;

  return Error::OK;
}


/**
 * CheckSequencer - just return OK for now
 */
int Session::check_sequencer(struct LockSequencerT &sequencer) {
  LOG_WARN("CheckSequencer not implemented.");
  return Error::OK;
}



/**
 * Blocking 'status' method
 */
int Session::status() {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( Protocol::create_status_request() );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      error = (int)Protocol::response_code(eventPtr);
      LOG_VA_ERROR("Hyperspace 'status' error : %s", Error::get_text(error));
      if (m_verbose)
	LOG_VA_ERROR("%s", Protocol::string_format_message(eventPtr.get()).c_str());
    }
  }
  return error;
}





/**
 * Transition session state
 */
int Session::state_transition(int state) {
  boost::mutex::scoped_lock lock(m_mutex);
  int oldState = m_state;
  m_state = state;
  if (m_state == STATE_SAFE) {
    m_cond.notify_all();
    if (m_session_callback && oldState == STATE_JEOPARDY)
      m_session_callback->safe();
  }
  else if (m_state == STATE_JEOPARDY) {
    if (m_session_callback && oldState == STATE_SAFE) {
      m_session_callback->jeopardy();
      boost::xtime_get(&m_expire_time, boost::TIME_UTC);
      m_expire_time.sec += m_grace_period;
    }
  }
  else if (m_state == STATE_EXPIRED) {
    if (m_session_callback && oldState != STATE_EXPIRED)
      m_session_callback->expired();
    m_cond.notify_all();
  }
  return oldState;
}



/**
 * Return Sesion state
 */
int Session::get_state() {
  boost::mutex::scoped_lock lock(m_mutex);
  return m_state;
}


/**
 * Return true if session expired
 */
bool Session::expired() {
  boost::mutex::scoped_lock lock(m_mutex);
  boost::xtime now;
  boost::xtime_get(&now, boost::TIME_UTC);
  if (xtime_cmp(m_expire_time, now) < 0)
    return true;
  return false;
}


/**
 * Waits for session state to change to SAFE
 */
bool Session::wait_for_connection(long maxWaitSecs) {
  boost::mutex::scoped_lock lock(m_mutex);
  boost::xtime dropTime, now;

  boost::xtime_get(&dropTime, boost::TIME_UTC);
  dropTime.sec += maxWaitSecs;

  while (m_state != STATE_SAFE) {
    m_cond.timed_wait(lock, dropTime);
    boost::xtime_get(&now, boost::TIME_UTC);
    if (xtime_cmp(now, dropTime) >= 0)
      return false;
  }
  return true;
}



bool Session::wait_for_safe() {
  boost::mutex::scoped_lock lock(m_mutex);
  while (m_state != STATE_SAFE) {
    if (m_state == STATE_EXPIRED)
      return false;
    m_cond.wait(lock);
  }
  return true;
}

int Session::send_message(CommBufPtr &cbufPtr, DispatchHandler *handler) {
  int error;

  if ((error = m_comm->send_request(m_master_addr, m_timeout, cbufPtr, handler)) != Error::OK) {
    std::string str;
    LOG_VA_WARN("Comm::send_request to Hypertable.Master at %s failed - %s",
		InetAddr::string_format(str, m_master_addr), Error::get_text(error));
  }
  return error;
}


/**
 *
 */
void Session::normalize_name(std::string name, std::string &normal) {

  if (name == "/") {
    normal = name;
    return;
  }

  normal = "";
  if (name[0] != '/')
    normal += "/";

  if (name.find('/', name.length()-1) == string::npos)
    normal += name;
  else
    normal += name.substr(0, name.length()-1);
}
