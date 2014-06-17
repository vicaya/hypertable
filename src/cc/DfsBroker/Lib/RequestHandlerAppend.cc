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
#include "Common/Filesystem.h"
#include "Common/Logger.h"

#include "AsyncComm/CommHeader.h"

#include "Common/Serialization.h"

#include "RequestHandlerAppend.h"
#include "ResponseCallbackAppend.h"

using namespace Hypertable;
using namespace DfsBroker;
using namespace Serialization;


/**
 *
 */
void RequestHandlerAppend::run() {
  ResponseCallbackAppend cb(m_comm, m_event_ptr);
  const uint8_t *decode_ptr = m_event_ptr->payload;
  size_t decode_remain = m_event_ptr->payload_len;

  try {
    uint32_t fd = decode_i32(&decode_ptr, &decode_remain);
    uint32_t amount = decode_i32(&decode_ptr, &decode_remain);
    bool flush = decode_bool(&decode_ptr, &decode_remain);

    if (decode_remain < amount)
      HT_THROW_INPUT_OVERRUN(decode_remain, amount);

    decode_ptr += HT_DIRECT_IO_ALIGNMENT - 9;

    m_broker->append(&cb, fd, amount, decode_ptr, flush);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), "Error handling APPEND message");
  }
}
