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
#include "Common/Error.h"
#include "Common/Logger.h"

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
  size_t remaining = m_event_ptr->message_len - 2;
  const uint8_t *msg = m_event_ptr->message + 2;

  try {
    uint32_t fd = decode_i32(&msg, &remaining);
    uint32_t amount = decode_i32(&msg, &remaining);
    bool flush = decode_bool(&msg, &remaining);

    if (remaining < amount)
      HT_THROW_INPUT_OVERRUN(remaining, amount);

    m_broker->append(&cb, fd, amount, msg, flush);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), "Error handling APPEND message");
  }
}
