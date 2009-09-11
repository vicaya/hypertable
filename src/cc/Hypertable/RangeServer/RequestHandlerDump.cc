/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

#include "RequestHandlerDump.h"
#include "RangeServer.h"

using namespace Hypertable;

/**
 *
 */
void RequestHandlerDump::run() {
  ResponseCallback cb(m_comm, m_event_ptr);
  const uint8_t *decode_ptr = m_event_ptr->payload;
  size_t decode_remain = m_event_ptr->payload_len;
  const char *outfile = 0;
  bool nokeys;

  try {
    outfile = Serialization::decode_vstr(&decode_ptr, &decode_remain);
    nokeys = Serialization::decode_bool(&decode_ptr, &decode_remain);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(Error::PROTOCOL_ERROR, "Error decoding DUMP request");
    return;
  }

  m_range_server->dump(&cb, outfile, nokeys);
}
