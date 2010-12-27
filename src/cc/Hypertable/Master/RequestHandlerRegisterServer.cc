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
#include "Common/Serialization.h"
#include "Common/StatsSystem.h"

#include "AsyncComm/ResponseCallback.h"

#include "Master.h"
#include "RequestHandlerRegisterServer.h"
#include "ResponseCallbackRegisterServer.h"

using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
void RequestHandlerRegisterServer::run() {
  ResponseCallbackRegisterServer cb(m_comm, m_event_ptr);
  const uint8_t *decode_ptr = m_event_ptr->payload;
  size_t decode_remain = m_event_ptr->payload_len;
  StatsSystem stats;

  try {
    String location = decode_vstr(&decode_ptr, &decode_remain);
    uint16_t listen_port = decode_i16(&decode_ptr, &decode_remain);
    stats.decode(&decode_ptr, &decode_remain);
    m_master->register_server(&cb, location, listen_port, stats);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), "Error handling Register Server message");
  }
}
