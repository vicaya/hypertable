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
#include "Common/Logger.h"

#include "AsyncComm/ResponseCallback.h"
#include "AsyncComm/Serialization.h"

#include "Master.h"
#include "RequestHandlerAttrSet.h"

using namespace Hyperspace;
using namespace Hypertable;

/**
 *
 */
void RequestHandlerAttrSet::run() {
  ResponseCallback cb(m_comm, m_event_ptr);
  size_t remaining = m_event_ptr->messageLen - 2;
  uint8_t *msgPtr = m_event_ptr->message + 2;
  const char *name;
  uint64_t handle;
  uint8_t *value;
  int32_t valueLen;

  // handle
  if (!Serialization::decode_long(&msgPtr, &remaining, &handle))
    goto abort;

  // name
  if (!Serialization::decode_string(&msgPtr, &remaining, &name))
    goto abort;

  // value
  if (!Serialization::decode_byte_array(&msgPtr, &remaining, &value, &valueLen))
    goto abort;

  m_master->attr_set(&cb, m_session_id, handle, name, value, valueLen);

  return;

 abort:
  HT_ERROR("Encoding problem with ATTRSET message");
  cb.error(Error::PROTOCOL_ERROR, "Encoding problem with ATTRSET message");
}
