/**
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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
#include "Common/Error.h"
#include "Common/Logger.h"

#include "AsyncComm/ResponseCallback.h"
#include "Common/Serialization.h"

#include "RequestHandlerRename.h"

using namespace Hypertable;
using namespace DfsBroker;
using namespace Serialization;

void
RequestHandlerRename::run() {
  ResponseCallback cb(m_comm, m_event_ptr);
  size_t remaining = m_event_ptr->messageLen - 2;
  uint8_t *msgp = m_event_ptr->message + 2;
  const char *src, *dst;

  try {
    if (!decode_string(&msgp, &remaining, &src) ||
        !decode_string(&msgp, &remaining, &dst))
      throw Exception(Error::PROTOCOL_ERROR, "Error decoding paths for rename");

    m_broker->rename(&cb, src, dst);
  }
  catch (Exception &e) {
    String msg = format("Encoding problem with Rename message: %s", e.what());
    HT_ERROR(msg.c_str());
    cb.error(e.code(), msg);
  }
}
