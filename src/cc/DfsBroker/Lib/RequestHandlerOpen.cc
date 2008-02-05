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

#include <cstring>

#include "Common/Error.h"
#include "Common/Logger.h"

#include "AsyncComm/ResponseCallback.h"
#include "AsyncComm/Serialization.h"

#include "RequestHandlerOpen.h"

using namespace Hypertable;
using namespace Hypertable::DfsBroker;

/**
 *
 */
void RequestHandlerOpen::run() {
  ResponseCallbackOpen cb(m_comm, m_event_ptr);
  const char *fileName;
  uint32_t bufferSize;
  size_t remaining = m_event_ptr->messageLen - sizeof(int16_t);
  uint8_t *msgPtr = m_event_ptr->message + sizeof(int16_t);

  // buffer size
  if (!Serialization::decode_int(&msgPtr, &remaining, &bufferSize))
    goto abort;

  // file name
  if (!Serialization::decode_string(&msgPtr, &remaining, &fileName))
    goto abort;

  // validate filename
  if (fileName[strlen(fileName)-1] == '/') {
    HT_ERRORF("open failed: bad filename - %s", fileName);
    cb.error(Error::DFSBROKER_BAD_FILENAME, fileName);
    return;
  }

  m_broker->open(&cb, fileName, bufferSize);

  return;

 abort:
  HT_ERROR("Encoding problem with OPEN message");
  cb.error(Error::PROTOCOL_ERROR, "Encoding problem with OPEN message");
  return;
}
