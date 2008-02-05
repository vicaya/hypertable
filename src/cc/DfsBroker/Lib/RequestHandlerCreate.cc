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

#include "RequestHandlerCreate.h"

using namespace Hypertable;
using namespace Hypertable::DfsBroker;

/**
 *
 */
void RequestHandlerCreate::run() {
  ResponseCallbackOpen cb(m_comm, m_event_ptr);
  const char *fileName;
  size_t remaining = m_event_ptr->messageLen - 2;
  uint8_t *msgPtr = m_event_ptr->message + 2;
  uint16_t sval, replication;
  uint32_t ival, bufferSize;
  uint64_t blockSize;
  bool overwrite;

  if (remaining < 18)
    goto abort;

  // overwrite flag
  Serialization::decode_short(&msgPtr, &remaining, &sval);
  overwrite = (sval == 0) ? false : true;

  // replication
  Serialization::decode_int(&msgPtr, &remaining, &ival);
  replication = (short)ival;

  // buffer size
  Serialization::decode_int(&msgPtr, &remaining, &bufferSize);

  // block size
  Serialization::decode_long(&msgPtr, &remaining, &blockSize);

  // file name
  if (!Serialization::decode_string(&msgPtr, &remaining, &fileName))
    goto abort;

  // validate filename
  if (fileName[strlen(fileName)-1] == '/') {
    HT_ERRORF("open failed: bad filename - %s", fileName);
    cb.error(Error::DFSBROKER_BAD_FILENAME, fileName);
    return;
  }

  m_broker->create(&cb, fileName, overwrite, bufferSize, replication, blockSize);

  return;

 abort:
  HT_ERROR("Encoding problem with CREATE message");
  cb.error(Error::PROTOCOL_ERROR, "Encoding problem with CREATE message");
  return;
}
