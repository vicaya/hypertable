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

#include "Common/Error.h"

#include "AsyncComm/Protocol.h"
#include "AsyncComm/Serialization.h"

#include "ScanBlock.h"

using namespace Hypertable;


/**
 *
 */
ScanBlock::ScanBlock() : m_flags(0), m_scanner_id(-1) {
  m_iter = m_vec.end();
}


/**
 *
 */
int ScanBlock::load(EventPtr &eventPtr) {
  uint8_t *msgPtr = eventPtr->message + 4;
  uint8_t *endPtr;
  size_t remaining = eventPtr->messageLen - 4;
  uint32_t len;
  ByteString32T *key, *value;

  m_event_ptr = eventPtr;

  m_vec.clear();
  m_iter = m_vec.end();

  if ((m_error = (int)Protocol::response_code(eventPtr)) != Error::OK)
    return m_error;

  if (!(Serialization::decode_short(&msgPtr, &remaining, &m_flags))) {
    m_error = Error::RESPONSE_TRUNCATED;
    return m_error;
  }

  if (!(Serialization::decode_int(&msgPtr, &remaining, (uint32_t *)&m_scanner_id))) {
    m_error = Error::RESPONSE_TRUNCATED;
    return m_error;
  }

  if (!(Serialization::decode_int(&msgPtr, &remaining, &len))) {
    m_error = Error::RESPONSE_TRUNCATED;
    return m_error;
  }

  endPtr = msgPtr + len;

  while (msgPtr < endPtr) {
    key = (ByteString32T *)msgPtr;
    msgPtr += 4 + key->len;
    value = (ByteString32T *)msgPtr;
    msgPtr += 4 + value->len;
    m_vec.push_back(std::make_pair(key, value));
  }

  m_iter = m_vec.begin();

  return m_error;
}


bool ScanBlock::next(const ByteString32T *&key, const ByteString32T *&value) {

  assert(m_error == Error::OK);

  if (m_iter == m_vec.end())
    return false;

  key = (*m_iter).first;
  value = (*m_iter).second;

  m_iter++;

  return true;
}
