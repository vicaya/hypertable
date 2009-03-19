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


void UpdateSchemaDispatchHandler::add(struct sockaddr_in &addr) {
  ScopedLock lock(m_mutex);
  String addr_str = InetAddr::format(addr);

  try {
    m_pending_addrs[addr_str] = &addr;
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
  String addr_str;

  if (event_ptr->type == Event::MESSAGE) {
    if ((result.error = Protocol::response_code(event_ptr)) != Error::OK) {
      result.addr = event_ptr->addr;
      result.msg = Protocol::string_format_message(event_ptr);
      m_errors.push_back(result);
    }
    else {
      // Successful response
      addr_str = InetAddr::format(event_ptr->addr);
      if (m_pending_addrs.erase(addr_str) == 0) {
        String expected_addrs;
        std::map<String, struct sockaddr_in *>::iterator iter;

        for(iter=m_pending_addrs.begin(); iter != m_pending_addrs.end();
            ++iter) {
          expected_addrs += "'" + iter->first + "' ";
        }
        HT_FATAL_OUT << "Received 'update schema ack' from unexpected addr '"
            << addr_str << "' expected one of " << expected_addrs << HT_END ;
      }
    }
  }
  else {
    result.error = event_ptr->error;
    result.addr = event_ptr->addr;
    result.msg = "";
    m_errors.push_back(result);
  }
  m_outstanding--;

  if (m_outstanding == 0)
    m_cond.notify_all();
}


void UpdateSchemaDispatchHandler::retry() {
  ScopedLock lock(m_mutex);
  std::map<String, struct sockaddr_in *>::iterator iter;

  try {
    m_errors.clear();
    for(iter=m_pending_addrs.begin(); iter != m_pending_addrs.end(); ++iter) {
      m_client.update_schema(*(iter->second), m_table, m_schema, this);
      m_outstanding++;
    }
  }
  catch (Exception &e) {
    ErrorResult result;
    result.addr = *(iter->second);
    result.error = e.code();
    result.msg = "Send error";
    m_errors.push_back(result);
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
