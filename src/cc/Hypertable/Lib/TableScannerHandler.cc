/** -*- C -*-
 * Copyright (C) 2009  Luke Lu (llu@hypertable.org)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include "Common/Compat.h"

#include "AsyncComm/Comm.h"

#include "TableScannerHandler.h"
#include "TableScannerAsync.h"

using namespace Hypertable;

void TableScannerHandler::run() {
  int32_t error;
  try {
    switch (m_event_ptr->type) {
    case(Event::MESSAGE):
      error = Protocol::response_code(m_event_ptr);
      if (error != Error::OK)
        m_scanner->handle_error(m_interval_scanner, error, m_event_ptr->to_str());
      else
        m_scanner->handle_result(m_interval_scanner, m_event_ptr);
      break;
    case(Event::TIMER):
      m_scanner->handle_timeout(m_interval_scanner, m_event_ptr->to_str());
      break;
    case(Event::ERROR):
      m_scanner->handle_error(m_interval_scanner, m_event_ptr->error, m_event_ptr->to_str());
      break;
    default:
      HT_INFO_OUT << "Received event " << m_event_ptr->to_str() << HT_END;
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
}
