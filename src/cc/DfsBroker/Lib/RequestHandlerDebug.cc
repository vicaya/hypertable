/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include "Common/Serialization.h"
#include "Common/StaticBuffer.h"

#include "RequestHandlerDebug.h"

using namespace Hypertable;
using namespace DfsBroker;
using namespace Serialization;


/**
 *
 */
void RequestHandlerDebug::run() {
  ResponseCallback cb(m_comm, m_event_ptr);
  const uint8_t *decode_ptr = m_event_ptr->payload;
  size_t decode_remain = m_event_ptr->payload_len;

  try {
    int32_t command = (int32_t)decode_i32(&decode_ptr, &decode_remain);
    StaticBuffer serialized_params;

    serialized_params.base = (uint8_t *)decode_ptr;
    serialized_params.size = decode_remain;
    serialized_params.own = false;

    m_broker->debug(&cb, command, serialized_params);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), "Error handling DEBUG message");
  }
}
