/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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
#include "Common/Error.h"
#include "Common/Logger.h"

#include "AsyncComm/Protocol.h"
#include "Common/Serialization.h"

#include "ScanBlock.h"

using namespace Hypertable;
using namespace Serialization;


/**
 *
 */
ScanBlock::ScanBlock() : m_flags(0), m_scanner_id(-1) {
  m_iter = m_vec.end();
}


/**
 *
 */
int ScanBlock::load(EventPtr &event_ptr) {
  const uint8_t *msg = event_ptr->message + 4;
  size_t remaining = event_ptr->message_len - 4;
  uint32_t len;

  m_event_ptr = event_ptr;
  m_vec.clear();
  m_iter = m_vec.end();

  if ((m_error = (int)Protocol::response_code(event_ptr)) != Error::OK)
    return m_error;

  try {
    m_flags = decode_i16(&msg, &remaining);
    m_scanner_id = decode_i32(&msg, &remaining);
    len = decode_i32(&msg, &remaining);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return e.code();
  }
  uint8_t *p = (uint8_t *)msg;
  uint8_t *endp = p + len;
  SerializedKey key;
  ByteString value;

  while (p < endp) {
    key.ptr = p;
    p += key.length();
    value.ptr = p;
    p += value.length();
    m_vec.push_back(std::make_pair(key, value));
  }
  m_iter = m_vec.begin();

  return m_error;
}


bool ScanBlock::next(SerializedKey &key, ByteString &value) {

  assert(m_error == Error::OK);

  if (m_iter == m_vec.end())
    return false;

  key = (*m_iter).first;
  value = (*m_iter).second;

  m_iter++;

  return true;
}
