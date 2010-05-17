/** -*- c++ -*-
 * Copyright (C) 2010 Sanjit Jhala(Hypertable, Inc.)
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

#include "DispatchHandlerGetStatistics.h"

using namespace Hypertable;


DispatchHandlerGetStatistics::DispatchHandlerGetStatistics(Comm *comm, time_t timeout)
  : m_outstanding(0), m_client(comm, timeout) {
}


void DispatchHandlerGetStatistics::add(const CommAddress &addr, bool all, bool snapshot) {
  ScopedLock lock(m_mutex);

  try {
    m_pending.insert(addr);
    m_client.get_statistics(addr, all, snapshot, this);
    m_outstanding++;
  }
  catch (Exception &e) {
    ErrorResult result;
    result.addr = addr;
    result.error = e.code();
    result.msg = "Send error";
    m_errors.push_back(result);
  }
}


void DispatchHandlerGetStatistics::handle(EventPtr &event_ptr) {
  ScopedLock lock(m_mutex);
  ErrorResult result;

  result.addr.set_inet(event_ptr->addr);

  if (event_ptr->type == Event::MESSAGE) {
    if ((result.error = Protocol::response_code(event_ptr)) != Error::OK) {
      result.msg = Protocol::string_format_message(event_ptr);
      m_errors.push_back(result);
    }
    else {
      // Unexpected response
      if (m_pending.erase(result.addr) == 0) {
        HT_ERROR_OUT << "Received 'get_statistics' response from unexpected location '"
		          << result.addr.to_str() << "'" << HT_END ;
      }
      else {
        // Store succesful response
        m_responses[event_ptr->addr] = event_ptr;
      }
    }
  }
  else {
    result.error = event_ptr->error;
    result.msg = "";
    m_errors.push_back(result);
  }
  m_outstanding--;

  if (m_outstanding == 0)
    m_cond.notify_all();
}


bool DispatchHandlerGetStatistics::wait_for_completion() {
  ScopedLock lock(m_mutex);
  while (m_outstanding > 0)
    m_cond.wait(lock);
  return m_errors.empty();
}


void DispatchHandlerGetStatistics::get_errors(std::vector<ErrorResult> &errors) {
  errors = m_errors;
}

void DispatchHandlerGetStatistics::get_responses(GetStatisticsResponseMap &responses) {
  responses = m_responses;
}
