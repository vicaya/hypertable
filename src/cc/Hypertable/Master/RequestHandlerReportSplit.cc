/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#include "Master.h"
#include "RequestHandlerReportSplit.h"

using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
void RequestHandlerReportSplit::run() {
  ResponseCallback cb(m_comm, m_event_ptr);
  const uint8_t *decode_ptr = m_event_ptr->payload;
  size_t decode_remain = m_event_ptr->payload_len;

  try {
    TableIdentifier table(&decode_ptr, &decode_remain);
    RangeSpec range(&decode_ptr, &decode_remain);
    const char *log_dir = decode_vstr(&decode_ptr, &decode_remain);
    uint64_t soft_limit = decode_i64(&decode_ptr, &decode_remain);

    m_master->report_split(&cb, table, range, log_dir, soft_limit);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), "Error handling ReportSplit message");
  }
}
