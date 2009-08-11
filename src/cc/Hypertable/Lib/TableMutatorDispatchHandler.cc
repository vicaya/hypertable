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
#include "AsyncComm/Protocol.h"

#include "Common/Error.h"
#include "Common/Logger.h"

#include "TableMutatorDispatchHandler.h"

using namespace Hypertable;
using namespace Serialization;


/**
 *
 */
TableMutatorDispatchHandler::TableMutatorDispatchHandler(
  TableMutatorSendBuffer *send_buffer, bool refresh_schema)
  : m_send_buffer(send_buffer), m_refresh_schema(refresh_schema) {
}



/**
 *
 */
void TableMutatorDispatchHandler::handle(EventPtr &event_ptr) {
  int32_t error;

  if (event_ptr->type == Event::MESSAGE) {
    error = Protocol::response_code(event_ptr);
    if (error != Error::OK) {
      if (error == Error::RANGESERVER_GENERATION_MISMATCH && m_refresh_schema)
        m_send_buffer->add_retries_all(true, error);
      else
        m_send_buffer->add_errors_all(error);
    }
    else {
      const uint8_t *decode_ptr = event_ptr->payload + 4;
      size_t decode_remain = event_ptr->payload_len - 4;
      uint32_t count, offset, len;

      if (decode_remain == 0) {
        m_send_buffer->clear();
      }
      else {
        while (decode_remain) {
          try {
            error = decode_i32(&decode_ptr, &decode_remain);
            count = decode_i32(&decode_ptr, &decode_remain);
            offset = decode_i32(&decode_ptr, &decode_remain);
            len = decode_i32(&decode_ptr, &decode_remain);
          }
          catch (Exception &e) {
            HT_ERROR_OUT << e << HT_END;
            break;
          }
          if (error == Error::RANGESERVER_OUT_OF_RANGE)
            m_send_buffer->add_retries(count, offset, len);
          else
            m_send_buffer->add_errors(error, count, offset, len);
        }
      }
    }
  }
  else if (event_ptr->type == Event::ERROR) {
    m_send_buffer->add_retries_all();
    HT_WARNF("%s, will retry ...", event_ptr->to_str().c_str());
  }
  else {
    // this should never happen
    HT_ERRORF("%s", event_ptr->to_str().c_str());
  }

  m_send_buffer->counterp->decrement();
}

