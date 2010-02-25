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

#include "DropTableDispatchHandler.h"

using namespace Hypertable;


/**
 *
 */
DropTableDispatchHandler::DropTableDispatchHandler(const TableIdentifier &table,
                                                   Comm *comm)
  : m_outstanding(0), m_client(comm), m_table_name(table.name) {
  memcpy(&m_table, &table, sizeof(TableIdentifier));
  m_table.name = m_table_name.c_str();
  return;
}



/**
 * Adds
 */
void DropTableDispatchHandler::add(const CommAddress &addr) {
  ScopedLock lock(m_mutex);

  try {
    m_client.drop_table(addr, m_table, this);
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



/**
 *
 */
void DropTableDispatchHandler::handle(EventPtr &event_ptr) {
  ScopedLock lock(m_mutex);
  ErrorResult result;

  result.addr.set_inet(event_ptr->addr);

  if (event_ptr->type == Event::MESSAGE) {
    if ((result.error = Protocol::response_code(event_ptr)) != Error::OK) {
      result.msg = Protocol::string_format_message(event_ptr);
      m_errors.push_back(result);
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


/**
 *
 */
bool DropTableDispatchHandler::wait_for_completion() {
  ScopedLock lock(m_mutex);
  while (m_outstanding > 0)
    m_cond.wait(lock);
  return m_errors.empty();
}


/**
 *
 */
void DropTableDispatchHandler::get_errors(std::vector<ErrorResult> &errors) {
  errors = m_errors;
}
