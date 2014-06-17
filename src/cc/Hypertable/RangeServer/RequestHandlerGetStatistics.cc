/** -*- c++ -*-
 * Copyright (C) 2008 Donald <donaldliew@gmail.com>
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
#include "AsyncComm/ResponseCallback.h"

#include "RequestHandlerGetStatistics.h"
#include "ResponseCallbackGetStatistics.h"

#include "RangeServer.h"

using namespace Hypertable;

/**
 *
 */
void RequestHandlerGetStatistics::run() {
  ResponseCallbackGetStatistics cb(m_comm, m_event_ptr);

  try {
    m_range_server->get_statistics(&cb);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), "Error handling GetStatistics message");
  }
}
