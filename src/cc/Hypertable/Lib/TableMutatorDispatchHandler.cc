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
#include "AsyncComm/Protocol.h"

#include "Common/Error.h"
#include "Common/Logger.h"

#include "TableMutatorDispatchHandler.h"

using namespace Hypertable;
using namespace Serialization;


/**
 *
 */
TableMutatorDispatchHandler::TableMutatorDispatchHandler(TableMutatorSendBuffer *send_buffer) : m_send_buffer(send_buffer) {
  return;
}



/**
 *
 */
void TableMutatorDispatchHandler::handle(EventPtr &event_ptr) {
  int32_t error;

  if (event_ptr->type == Event::MESSAGE) {
    error = Protocol::response_code(event_ptr);
    if (error != Error::OK) {
      m_send_buffer->add_errors_all(error);
    }
    else {
      const uint8_t *ptr = event_ptr->message + 4;
      size_t remaining = event_ptr->message_len - 4;
      uint32_t offset, len;

      if (remaining == 0) {
        m_send_buffer->clear();
      }
      else {
        while (remaining) {
          try {
            error = decode_i32(&ptr, &remaining);
            offset = decode_i32(&ptr, &remaining);
            len = decode_i32(&ptr, &remaining);
          }
          catch (Exception &e) {
            HT_ERROR_OUT << e << HT_END;
            break;
          }
          if (error == Error::RANGESERVER_OUT_OF_RANGE)
            m_send_buffer->add_retries(offset, len);
          else
            m_send_buffer->add_errors(error, offset, len);
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

