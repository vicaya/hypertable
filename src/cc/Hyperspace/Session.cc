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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
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
#include "Common/Serialization.h"

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/Properties.h"

#include "ClientHandleState.h"
#include "Master.h"
#include "Protocol.h"
#include "Session.h"

using namespace std;
using namespace Hypertable;
using namespace Hyperspace;
using namespace Serialization;


/**
 *
 */
Session::Session(Comm *comm, PropertiesPtr &props_ptr, SessionCallback *callback) : m_comm(comm), m_verbose(false), m_silent(false), m_state(STATE_JEOPARDY), m_session_callback(callback) {
  uint16_t master_port;
  const char *master_host;

  m_verbose = props_ptr->get_bool("Hypertable.Verbose", false);
  m_silent = props_ptr->get_bool("silent", false);
  master_host = props_ptr->get("Hyperspace.Master.Host", "localhost");
  master_port = (uint16_t)props_ptr->get_int("Hyperspace.Master.Port", Master::DEFAULT_MASTER_PORT);
  m_grace_period = (uint32_t)props_ptr->get_int("Hyperspace.GracePeriod", Master::DEFAULT_GRACEPERIOD);
  m_lease_interval = (uint32_t)props_ptr->get_int("Hyperspace.Lease.Interval", Master::DEFAULT_LEASE_INTERVAL);
  m_timeout = m_lease_interval * 2;

  if (!InetAddr::initialize(&m_master_addr, master_host, master_port))
    exit(1);

  boost::xtime_get(&m_expire_time, boost::TIME_UTC);
  m_expire_time.sec += m_grace_period;

  if (m_verbose) {
    cout << "Hyperspace.GracePeriod=" << m_grace_period << endl;
  }

  m_keepalive_handler_ptr = new ClientKeepaliveHandler(comm, props_ptr, this);
}

Session::~Session() {
}


int Session::open(ClientHandleStatePtr &handle_state, CommBufPtr &cbuf_ptr, uint64_t *handlep) {
  DispatchHandlerSynchronizer sync_handler;
  Hypertable::EventPtr event_ptr;

  handle_state->handle = 0;
  handle_state->sequencer = 0;
  handle_state->lock_status = 0;

  if ((handle_state->open_flags & OPEN_FLAG_LOCK_SHARED) == OPEN_FLAG_LOCK_SHARED)
    handle_state->lock_mode = LOCK_MODE_SHARED;
  else if ((handle_state->open_flags & OPEN_FLAG_LOCK_EXCLUSIVE) == OPEN_FLAG_LOCK_EXCLUSIVE)
    handle_state->lock_mode = LOCK_MODE_EXCLUSIVE;
  else
    handle_state->lock_mode = 0;

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbuf_ptr, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      error = (int)Protocol::response_code(event_ptr.get());
      if (!m_silent) {
        HT_ERRORF("Hyperspace 'open' error, name=%s flags=0x%x events=0x%x : "
            "%s", handle_state->normal_name.c_str(), handle_state->open_flags,
            handle_state->event_mask, Error::get_text(error));
        if (m_verbose)
          HT_ERRORF("%s", Protocol::string_format_message(event_ptr.get()).c_str());
      }
    }
    else {
      const uint8_t *ptr = event_ptr->message + 4;
      size_t remaining = event_ptr->message_len - 4;
      try {
        *handlep = decode_i64(&ptr, &remaining);
        decode_byte(&ptr, &remaining);
        handle_state->lock_generation = decode_i64(&ptr, &remaining);
        /** if (createdp) *createdp = cbyte ? true : false; **/
        handle_state->handle = *handlep;
        m_keepalive_handler_ptr->register_handle(handle_state);
      }
      catch (Exception &e) {
        HT_ERROR_OUT << e << HT_END;
        return e.code();
      }
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
int Session::open(const std::string &name, uint32_t flags, HandleCallbackPtr &callback, uint64_t *handlep) {
  ClientHandleStatePtr handle_state(new ClientHandleState());
  std::vector<Attribute> empty_attrs;

  handle_state->open_flags = flags;
  handle_state->event_mask = (callback) ? callback->get_event_mask() : 0;
  handle_state->callback = callback;
  normalize_name(name, handle_state->normal_name);

  CommBufPtr cbuf_ptr(Protocol::create_open_request(handle_state->normal_name, flags, callback, empty_attrs));

  return open(handle_state, cbuf_ptr, handlep);

}



/**
 *
 */
int Session::create(const std::string &name, uint32_t flags, HandleCallbackPtr &callback, std::vector<Attribute> &init_attrs, uint64_t *handlep) {
  ClientHandleStatePtr handle_state(new ClientHandleState());

  handle_state->open_flags = flags | OPEN_FLAG_CREATE | OPEN_FLAG_EXCL;
  handle_state->event_mask = (callback) ? callback->get_event_mask() : 0;
  handle_state->callback = callback;
  normalize_name(name, handle_state->normal_name);

  CommBufPtr cbuf_ptr(Protocol::create_open_request(handle_state->normal_name, handle_state->open_flags, callback, init_attrs));

  return open(handle_state, cbuf_ptr, handlep);
}



/**
 *
 */
int Session::close(uint64_t handle) {
  DispatchHandlerSynchronizer sync_handler;
  Hypertable::EventPtr event_ptr;
  CommBufPtr cbuf_ptr(Protocol::create_close_request(handle));

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbuf_ptr, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      error = (int)Protocol::response_code(event_ptr.get());
      if (!m_silent) {
        HT_ERRORF("Hyperspace 'close' error : %s", Error::get_text(error));
        if (m_verbose)
          HT_ERRORF("%s", Protocol::string_format_message(event_ptr.get()).c_str());
      }
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
int Session::mkdir(const std::string &name) {
  DispatchHandlerSynchronizer sync_handler;
  Hypertable::EventPtr event_ptr;
  std::string normal_name;

  normalize_name(name, normal_name);

  CommBufPtr cbuf_ptr(Protocol::create_mkdir_request(normal_name));

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbuf_ptr, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      error = (int)Protocol::response_code(event_ptr.get());
      if (!m_silent) {
        HT_ERRORF("Hyperspace 'mkdir' error, name=%s : %s", normal_name.c_str(), Error::get_text(error));
        if (m_verbose)
          HT_ERRORF("%s", Protocol::string_format_message(event_ptr.get()).c_str());
      }
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}


int Session::unlink(const std::string &name) {
  DispatchHandlerSynchronizer sync_handler;
  Hypertable::EventPtr event_ptr;
  std::string normal_name;

  normalize_name(name, normal_name);

  CommBufPtr cbuf_ptr(Protocol::create_delete_request(normal_name));

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbuf_ptr, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      error = (int)Protocol::response_code(event_ptr.get());
      if (!m_silent) {
        HT_ERRORF("Hyperspace 'delete' error, name=%s : %s", normal_name.c_str(), Error::get_text(error));
        if (m_verbose)
          HT_ERRORF("%s", Protocol::string_format_message(event_ptr.get()).c_str());
      }
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}


int Session::exists(const std::string &name, bool *existsp) {
  DispatchHandlerSynchronizer sync_handler;
  Hypertable::EventPtr event_ptr;
  std::string normal_name;

  normalize_name(name, normal_name);

  CommBufPtr cbuf_ptr(Protocol::create_exists_request(normal_name));

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbuf_ptr, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      error = (int)Protocol::response_code(event_ptr.get());
      if (!m_silent) {
        HT_ERRORF("Hyperspace 'exists' error, name=%s : %s", normal_name.c_str(), Error::get_text(error));
        if (m_verbose)
          HT_ERRORF("%s", Protocol::string_format_message(event_ptr.get()).c_str());
      }
    }
    else {
      const uint8_t *ptr = event_ptr->message + 4;
      size_t remaining = event_ptr->message_len - 4;
      try {
        uint8_t bval = decode_byte(&ptr, &remaining);
        *existsp = (bval == 0) ? false : true;
      }
      catch (Exception &e) {
        HT_ERROR_OUT << e << HT_END;
        return e.code();
      }
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}



/**
 */
int Session::attr_set(uint64_t handle, const std::string &name, const void *value, size_t value_len) {
  DispatchHandlerSynchronizer sync_handler;
  Hypertable::EventPtr event_ptr;
  CommBufPtr cbuf_ptr(Protocol::create_attr_set_request(handle, name, value, value_len));

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbuf_ptr, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      error = (int)Protocol::response_code(event_ptr.get());
      if (!m_silent) {
        HT_ERRORF("Hyperspace 'attrset' error, name=%s : %s", name.c_str(), Error::get_text(error));
        if (m_verbose)
          HT_ERRORF("%s", Protocol::string_format_message(event_ptr.get()).c_str());
      }
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
int Session::attr_get(uint64_t handle, const std::string &name, DynamicBuffer &value) {
  DispatchHandlerSynchronizer sync_handler;
  Hypertable::EventPtr event_ptr;
  CommBufPtr cbuf_ptr(Protocol::create_attr_get_request(handle, name));

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbuf_ptr, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      error = (int)Protocol::response_code(event_ptr.get());
      if (!m_silent) {
        HT_ERRORF("Hyperspace 'attrget' error, name=%s : %s", name.c_str(), Error::get_text(error));
        if (m_verbose)
          HT_ERRORF("%s", Protocol::string_format_message(event_ptr.get()).c_str());
      }
    }
    else {
      uint32_t attr_val_len = 0;
      const uint8_t *ptr = event_ptr->message + 4;
      size_t remaining = event_ptr->message_len - 4;
      try {
        void *attr_val = decode_bytes32(&ptr, &remaining, &attr_val_len);
        value.clear();
        value.ensure(attr_val_len+1);
        value.add_unchecked(attr_val, attr_val_len);
        // nul-terminate to make caller's lives easier
        *value.ptr = 0;
      }
      catch (Exception &e) {
        HT_ERROR_OUT << e << HT_END;
        return e.code();
      }
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
int Session::attr_del(uint64_t handle, const std::string &name) {
  DispatchHandlerSynchronizer sync_handler;
  Hypertable::EventPtr event_ptr;
  CommBufPtr cbuf_ptr(Protocol::create_attr_del_request(handle, name));

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbuf_ptr, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      error = (int)Protocol::response_code(event_ptr.get());
      if (!m_silent) {
        HT_ERRORF("Hyperspace 'attrdel' error, name=%s : %s", name.c_str(), Error::get_text(error));
        if (m_verbose)
          HT_ERRORF("%s", Protocol::string_format_message(event_ptr.get()).c_str());
      }
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}


int Session::readdir(uint64_t handle, std::vector<DirEntry> &listing) {
  DispatchHandlerSynchronizer sync_handler;
  Hypertable::EventPtr event_ptr;
  CommBufPtr cbuf_ptr(Protocol::create_readdir_request(handle));

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbuf_ptr, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      error = (int)Protocol::response_code(event_ptr.get());
      if (!m_silent) {
        HT_ERRORF("Hyperspace 'readdir' error : %s", Error::get_text(error));
        if (m_verbose)
          HT_ERRORF("%s", Protocol::string_format_message(event_ptr.get()).c_str());
      }
    }
    else {
      const uint8_t *ptr = event_ptr->message + 4;
      size_t remaining = event_ptr->message_len - 4;
      uint32_t entry_cnt;
      DirEntry dentry;
      try {
        entry_cnt = decode_i32(&ptr, &remaining);
      }
      catch (Exception &e) {
        if (!m_silent)
          HT_ERROR_OUT << e << HT_END;
        return Error::PROTOCOL_ERROR;
      }
      listing.clear();
      for (uint32_t i=0; i<entry_cnt; i++) {
        try {
          decode_dir_entry(&ptr, &remaining, dentry);
        }
        catch (Exception &e) {
          if (!m_silent)
            HT_ERRORF("Problem decoding entry %d of READDIR return packet: "
                      "%s - %s", i, e.what(), Error::get_text(e.code()));
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



int Session::lock(uint64_t handle, uint32_t mode, LockSequencer *sequencerp) {
  DispatchHandlerSynchronizer sync_handler;
  Hypertable::EventPtr event_ptr;
  CommBufPtr cbuf_ptr(Protocol::create_lock_request(handle, mode, false));
  ClientHandleStatePtr handle_state;
  uint32_t status = 0;

  if (!m_keepalive_handler_ptr->get_handle_state(handle, handle_state))
    return Error::HYPERSPACE_INVALID_HANDLE;

  if (handle_state->lock_status != 0)
    return Error::HYPERSPACE_ALREADY_LOCKED;

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  {
    boost::mutex::scoped_lock lock(handle_state->mutex);
    sequencerp->mode = mode;
    sequencerp->name = handle_state->normal_name;
    handle_state->sequencer = sequencerp;
  }

  int error = send_message(cbuf_ptr, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      error = (int)Protocol::response_code(event_ptr.get());
      if (!m_silent) {
        HT_ERRORF("Hyperspace 'lock' error, name='%s' : %s", handle_state->normal_name.c_str(), Error::get_text(error));
        if (m_verbose)
          HT_ERRORF("%s", Protocol::string_format_message(event_ptr.get()).c_str());
      }
    }
    else {
      boost::mutex::scoped_lock lock(handle_state->mutex);
      const uint8_t *ptr = event_ptr->message + 4;
      size_t remaining = event_ptr->message_len - 4;
      handle_state->lock_mode = mode;
      try {
        status = decode_i32(&ptr, &remaining);

        if (status == LOCK_STATUS_GRANTED) {
          sequencerp->generation = decode_i64(&ptr, &remaining);
          handle_state->lock_generation = sequencerp->generation;
          handle_state->lock_status = LOCK_STATUS_GRANTED;
        }
        else {
          assert(status == LOCK_STATUS_PENDING);
          handle_state->lock_status = LOCK_STATUS_PENDING;
          while (handle_state->lock_status == LOCK_STATUS_PENDING)
            handle_state->cond.wait(lock);
          if (handle_state->lock_status == LOCK_STATUS_CANCELLED)
            return Error::HYPERSPACE_REQUEST_CANCELLED;
          assert(handle_state->lock_status == LOCK_STATUS_GRANTED);
        }
      }
      catch (Exception &e) {
        HT_ERROR_OUT << e << HT_END;
        return e.code();
      }
    }
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}



int Session::try_lock(uint64_t handle, uint32_t mode, uint32_t *statusp, LockSequencer *sequencerp) {
  DispatchHandlerSynchronizer sync_handler;
  Hypertable::EventPtr event_ptr;
  CommBufPtr cbuf_ptr(Protocol::create_lock_request(handle, mode, true));
  ClientHandleStatePtr handle_state;

  if (!m_keepalive_handler_ptr->get_handle_state(handle, handle_state))
    return Error::HYPERSPACE_INVALID_HANDLE;

  if (handle_state->lock_status != 0)
    return Error::HYPERSPACE_ALREADY_LOCKED;

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbuf_ptr, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      error = (int)Protocol::response_code(event_ptr.get());
      if (!m_silent) {
        HT_ERRORF("Hyperspace 'trylock' error, name='%s' : %s", handle_state->normal_name.c_str(), Error::get_text(error));
        if (m_verbose)
          HT_ERRORF("%s", Protocol::string_format_message(event_ptr.get()).c_str());
      }
    }
    else {
      boost::mutex::scoped_lock lock(handle_state->mutex);
      const uint8_t *ptr = event_ptr->message + 4;
      size_t remaining = event_ptr->message_len - 4;
      try {
        *statusp = decode_i32(&ptr, &remaining);

        if (*statusp == LOCK_STATUS_GRANTED) {
          sequencerp->generation = decode_i64(&ptr, &remaining);
          sequencerp->mode = mode;
          sequencerp->name = handle_state->normal_name;
          handle_state->lock_mode = mode;
          handle_state->lock_status = LOCK_STATUS_GRANTED;
          handle_state->lock_generation = sequencerp->generation;
          handle_state->sequencer = 0;
        }
      }
      catch (Exception &e) {
        HT_ERROR_OUT << e << HT_END;
        return e.code();
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
  DispatchHandlerSynchronizer sync_handler;
  Hypertable::EventPtr event_ptr;
  CommBufPtr cbuf_ptr(Protocol::create_release_request(handle));
  ClientHandleStatePtr handle_state;

  if (!m_keepalive_handler_ptr->get_handle_state(handle, handle_state))
    return Error::HYPERSPACE_INVALID_HANDLE;

 try_again:
  if (!wait_for_safe())
    return Error::HYPERSPACE_EXPIRED_SESSION;

  int error = send_message(cbuf_ptr, &sync_handler);
  if (error == Error::OK) {
    boost::mutex::scoped_lock lock(handle_state->mutex);
    if (!sync_handler.wait_for_reply(event_ptr)) {
      error = (int)Protocol::response_code(event_ptr.get());
      if (!m_silent) {
        HT_ERRORF("Hyperspace 'release' error : %s", Error::get_text(error));
        if (m_verbose)
          HT_ERRORF("%s", Protocol::string_format_message(event_ptr.get()).c_str());
      }
    }
    handle_state->lock_status = 0;
    handle_state->cond.notify_all();
  }
  else {
    state_transition(Session::STATE_JEOPARDY);
    goto try_again;
  }

  return error;
}



int Session::get_sequencer(uint64_t handle, LockSequencer *sequencerp) {
  ClientHandleStatePtr handle_state;

  if (!m_keepalive_handler_ptr->get_handle_state(handle, handle_state))
    return Error::HYPERSPACE_INVALID_HANDLE;

  if (handle_state->lock_generation == 0)
    return Error::HYPERSPACE_NOT_LOCKED;

  sequencerp->name = handle_state->normal_name;
  sequencerp->mode = handle_state->lock_mode;
  sequencerp->generation = handle_state->lock_generation;

  return Error::OK;
}


/**
 */
int Session::check_sequencer(LockSequencer &sequencer) {
  HT_WARN("CheckSequencer not implemented.");
  return Error::OK;
}



/**
 */
int Session::status() {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbuf_ptr(Protocol::create_status_request());
  int error = send_message(cbuf_ptr, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      error = (int)Protocol::response_code(event_ptr);
      if (!m_silent) {
        HT_ERRORF("Hyperspace 'status' error : %s", Error::get_text(error));
        if (m_verbose)
          HT_ERRORF("%s", Protocol::string_format_message(event_ptr.get()).c_str());
      }
    }
  }
  return error;
}





/**
 */
int Session::state_transition(int state) {
  boost::mutex::scoped_lock lock(m_mutex);
  int old_state = m_state;
  m_state = state;
  if (m_state == STATE_SAFE) {
    m_cond.notify_all();
    if (m_session_callback && old_state == STATE_JEOPARDY)
      m_session_callback->safe();
  }
  else if (m_state == STATE_JEOPARDY) {
    if (m_session_callback && old_state == STATE_SAFE) {
      m_session_callback->jeopardy();
      boost::xtime_get(&m_expire_time, boost::TIME_UTC);
      m_expire_time.sec += m_grace_period;
    }
  }
  else if (m_state == STATE_EXPIRED) {
    if (m_session_callback && old_state != STATE_EXPIRED)
      m_session_callback->expired();
    m_cond.notify_all();
  }
  return old_state;
}



/**
 */
int Session::get_state() {
  boost::mutex::scoped_lock lock(m_mutex);
  return m_state;
}


/**
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
 */
bool Session::wait_for_connection(long max_wait_secs) {
  boost::mutex::scoped_lock lock(m_mutex);
  boost::xtime drop_time, now;

  boost::xtime_get(&drop_time, boost::TIME_UTC);
  drop_time.sec += max_wait_secs;

  while (m_state != STATE_SAFE) {
    m_cond.timed_wait(lock, drop_time);
    boost::xtime_get(&now, boost::TIME_UTC);
    if (xtime_cmp(now, drop_time) >= 0)
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

int Session::send_message(CommBufPtr &cbuf_ptr, DispatchHandler *handler) {
  int error;

  if ((error = m_comm->send_request(m_master_addr, m_timeout, cbuf_ptr, handler)) != Error::OK) {
    std::string str;
    if (!m_silent)
      HT_WARNF("Comm::send_request to Hypertable.Master at %s failed - %s",
                  InetAddr::string_format(str, m_master_addr), Error::get_text(error));
  }
  return error;
}


/**
 *
 */
void Session::normalize_name(const std::string &name, std::string &normal) {

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
