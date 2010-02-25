/** -*- c++ -*-
 * Copyright (C) 2009 Sanjit Jhala(Zvents, Inc.)
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

#include "UpdateSchemaDispatchHandler.h"

using namespace Hypertable;


UpdateSchemaDispatchHandler::UpdateSchemaDispatchHandler(
    const TableIdentifier &table, const char *schema, Comm *comm,
    time_t timeout)
  : m_outstanding(0), m_client(comm, timeout), m_schema(schema),
  m_table_name(table.name) {
  memcpy(&m_table, &table, sizeof(TableIdentifier));
  m_table.name = m_table_name.c_str();
  return;
}


void UpdateSchemaDispatchHandler::add(const CommAddress &addr) {
  ScopedLock lock(m_mutex);

  try {
    m_pending.insert(addr);
    m_client.update_schema(addr, m_table, m_schema, this);
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


void UpdateSchemaDispatchHandler::handle(EventPtr &event_ptr) {
  ScopedLock lock(m_mutex);
  ErrorResult result;

  result.addr.set_inet(event_ptr->addr);

  if (event_ptr->type == Event::MESSAGE) {
    if ((result.error = Protocol::response_code(event_ptr)) != Error::OK) {
      result.msg = Protocol::string_format_message(event_ptr);
      m_errors.push_back(result);
    }
    else {
      // Successful response
      if (m_pending.erase(result.addr) == 0) {
        HT_FATAL_OUT << "Received 'update schema ack' from unexpected location '"
		     << result.addr.to_str() << "'" << HT_END ;
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


void UpdateSchemaDispatchHandler::retry() {
  ScopedLock lock(m_mutex);

  m_errors.clear();

  foreach (CommAddress addr, m_pending) {
    try {
      m_client.update_schema(addr, m_table, m_schema, this);
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

}


bool UpdateSchemaDispatchHandler::wait_for_completion() {
  ScopedLock lock(m_mutex);
  while (m_outstanding > 0)
    m_cond.wait(lock);
  return m_errors.empty();
}


void UpdateSchemaDispatchHandler::get_errors(std::vector<ErrorResult> &errors) {
  errors = m_errors;
}
