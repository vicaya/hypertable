/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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
#include "Common/Serialization.h"

#include <ctime>

#include "RangeServerConnection.h"

using namespace Hypertable;

RangeServerConnection::RangeServerConnection(MetaLog::WriterPtr &mml_writer, const String &location,
                                             const String &hostname, InetAddr public_addr) 
  : MetaLog::Entity(MetaLog::EntityType::RANGE_SERVER_CONNECTION), m_mml_writer(mml_writer),
    m_location(location), m_hostname(hostname), m_state(RangeServerConnectionState::REGISTERED), m_removal_time(0),
    m_public_addr(public_addr), m_connected(false) {
  m_comm_addr.set_proxy(m_location);
  m_mml_writer->record_state(this);
}

RangeServerConnection::RangeServerConnection(MetaLog::WriterPtr &mml_writer, const MetaLog::EntityHeader &header_)
  : MetaLog::Entity(header_), m_mml_writer(mml_writer), m_connected(false) {
  m_comm_addr.set_proxy(m_location);
}

bool RangeServerConnection::connect(const String &hostname, InetAddr local_addr, InetAddr public_addr) {
  ScopedLock lock(m_mutex);
  m_hostname = hostname;
  m_local_addr = local_addr;
  m_public_addr = public_addr;
  if (!m_connected) {
    m_connected = true;
    m_cond.notify_all();
    return true;
  }
  m_cond.notify_all();
  return false;
}

bool RangeServerConnection::disconnect() {
  ScopedLock lock(m_mutex);
  if (m_connected) {
    m_connected = false;
    return true;
  }
  return false;
}

void RangeServerConnection::remove() {
  ScopedLock lock(m_mutex);
  m_connected = false;
  m_state = RangeServerConnectionState::REMOVED;
  m_removal_time = time(0);
  m_mml_writer->record_state(this);
  m_cond.notify_all();
}

bool RangeServerConnection::removed() {
  ScopedLock lock(m_mutex);
  return m_state == RangeServerConnectionState::REMOVED;
}

bool RangeServerConnection::wait_for_connection() {
  ScopedLock lock(m_mutex);
  boost::xtime expire_time;
  if (m_state == RangeServerConnectionState::REMOVED)
    return false;
  boost::xtime_get(&expire_time, boost::TIME_UTC);
  while (!m_connected) {
    expire_time.sec += (int64_t)60;
    HT_INFOF("Waiting for connection to '%s' ...", m_location.c_str());
    if (m_cond.timed_wait(lock, expire_time))
      break;
  }
  return true;
}

CommAddress RangeServerConnection::get_comm_address() {
  ScopedLock lock(m_mutex);
  return m_comm_addr;
}

void RangeServerConnection::display(std::ostream &os) {
  os << " " << m_location << " ";
}

size_t RangeServerConnection::encoded_length() const {
  return 8 + m_public_addr.encoded_length() +
    Serialization::encoded_length_vstr(m_location) +
    Serialization::encoded_length_vstr(m_hostname);
}

void RangeServerConnection::encode(uint8_t **bufp) const {
  Serialization::encode_i32(bufp, m_state);
  Serialization::encode_i32(bufp, m_removal_time);
  m_public_addr.encode(bufp);
  Serialization::encode_vstr(bufp, m_location);
  Serialization::encode_vstr(bufp, m_hostname);
}

void RangeServerConnection::decode(const uint8_t **bufp, size_t *remainp) {
  m_state = Serialization::decode_i32(bufp, remainp);
  m_removal_time = Serialization::decode_i32(bufp, remainp);
  m_public_addr.decode(bufp, remainp);
  m_location = Serialization::decode_vstr(bufp, remainp);
  m_hostname = Serialization::decode_vstr(bufp, remainp);
  m_comm_addr.set_proxy(m_location);
  m_connected = false;
}
