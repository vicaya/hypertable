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

#include "AsyncComm/Protocol.h"

#include "Common/Error.h"
#include "Common/Logger.h"

#include "MutatorDispatchHandler.h"

using namespace Hypertable;


/**
 *
 */
MutatorDispatchHandler::MutatorDispatchHandler(MutatorScatterBuffer::UpdateBuffer *update_buffer) : m_update_buffer(update_buffer) {
  return;
}



/**
 *
 */
void MutatorDispatchHandler::handle(EventPtr &event_ptr) {

  if (event_ptr->type == Event::MESSAGE) {
    m_update_buffer->error = Protocol::response_code(event_ptr);
    if (m_update_buffer->error == Error::RANGESERVER_PARTIAL_UPDATE)
      m_update_buffer->event_ptr = event_ptr;
    m_update_buffer->counterp->decrement(m_update_buffer->error != Error::OK);
  }
  else if (event_ptr->type == Event::ERROR) {
    m_update_buffer->error = event_ptr->error;
    m_update_buffer->counterp->decrement(true);
  }
  else {
    // this should never happen
    LOG_VA_INFO("%s", event_ptr->toString().c_str());
    m_update_buffer->counterp->decrement(false);
  }

}

