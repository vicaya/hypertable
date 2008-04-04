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

#include "Common/Error.h"
#include "Common/Logger.h"

#include "AsyncComm/ResponseCallback.h"
#include "AsyncComm/Serialization.h"

#include "Hypertable/Lib/Types.h"

#include "RangeServer.h"
#include "RequestHandlerLoadRange.h"

using namespace Hypertable;

/**
 *
 */
void RequestHandlerLoadRange::run() {
  ResponseCallback cb(m_comm, m_event_ptr);
  TableIdentifier table;
  RangeSpec range;
  const char *transfer_log_dir;
  uint64_t soft_limit;
  uint16_t flags;
  size_t remaining = m_event_ptr->messageLen - 2;
  uint8_t *msgPtr = m_event_ptr->message + 2;

  // Table
  if (!DecodeTableIdentifier(&msgPtr, &remaining, &table))
    goto abort;

  // Range
  if (!DecodeRange(&msgPtr, &remaining, &range))
    goto abort;

  // transfer_log_dir
  if (!Serialization::decode_string(&msgPtr, &remaining, &transfer_log_dir))
    goto abort;

  // soft_limit
  if (!Serialization::decode_long(&msgPtr, &remaining, &soft_limit))
    goto abort;

  // flags
  if (!Serialization::decode_short(&msgPtr, &remaining, &flags))
    goto abort;

  m_range_server->load_range(&cb, &table, &range, transfer_log_dir, soft_limit, flags);

  return;

 abort:
  HT_ERROR("Encoding problem with LoadRange message");
  cb.error(Error::PROTOCOL_ERROR, "Encoding problem with LoadRange message");
}
