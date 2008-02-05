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

#include "Hypertable/Lib/Types.h"

#include "Master.h"
#include "RequestHandlerReportSplit.h"

using namespace Hypertable;

/**
 *
 */
void RequestHandlerReportSplit::run() {
  ResponseCallback cb(m_comm, m_event_ptr);
  TableIdentifierT table;
  RangeT range;
  uint64_t soft_limit;
  size_t remaining = m_event_ptr->messageLen - 2;
  uint8_t *msgPtr = m_event_ptr->message + 2;

  // Table
  if (!DecodeTableIdentifier(&msgPtr, &remaining, &table))
    goto abort;

  // Range
  if (!DecodeRange(&msgPtr, &remaining, &range))
    goto abort;

  // soft_limit
  if (!Serialization::decode_long(&msgPtr, &remaining, &soft_limit))
    goto abort;

  m_master->report_split(&cb, table, range, soft_limit);

  return;

 abort:
  HT_ERROR("Encoding problem with ReportSplit message");
  cb.error(Error::PROTOCOL_ERROR, "Encoding problem with ReportSplit message");
}
