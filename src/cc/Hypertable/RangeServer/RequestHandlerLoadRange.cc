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

#include "AsyncComm/ResponseCallback.h"
#include "Common/Serialization.h"

#include "Hypertable/Lib/Types.h"

#include "RangeServer.h"
#include "RequestHandlerLoadRange.h"

using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
void RequestHandlerLoadRange::run() {
  ResponseCallback cb(m_comm, m_event_ptr);
  TableIdentifier table;
  RangeSpec range;
  RangeState range_state;
  const char *transfer_log_dir;
  uint16_t flags;
  size_t remaining = m_event_ptr->message_len - 2;
  const uint8_t *p = m_event_ptr->message + 2;

  try {
    table.decode(&p, &remaining);
    range.decode(&p, &remaining);
    transfer_log_dir = decode_str16(&p, &remaining);
    range_state.decode(&p, &remaining);
    flags = decode_i16(&p, &remaining);

    m_range_server->load_range(&cb, &table, &range, transfer_log_dir,
                               &range_state, flags);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(Error::PROTOCOL_ERROR, "Error handling LoadRange message");
  }
}
