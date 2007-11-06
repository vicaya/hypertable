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
#include <cassert>
#include <cstring>

#include "Common/Error.h"

#include "Hyperspace/Session.h"

#include "RootFileHandler.h"
#include "RangeLocator.h"


/**
 * 
 */
RangeLocator::RangeLocator(ConnectionManagerPtr &connManagerPtr, Hyperspace::SessionPtr &hyperspacePtr) : m_conn_manager_ptr(connManagerPtr), m_hyperspace_ptr(hyperspacePtr), m_cache(1000), m_root_stale(true), m_range_server(connManagerPtr->get_comm(), 30) {
  int error;
  
  m_root_handler_ptr = new RootFileHandler(this);

  if ((error = m_hyperspace_ptr->open("/hypertable/root", OPEN_FLAG_READ, m_root_handler_ptr, &m_root_file_handle)) != Error::OK) {
    LOG_VA_ERROR("Unable to open Hyperspace file '/hypertable/root' (%s)", Error::get_text(error));
    throw Exception(error);
  }

}

RangeLocator::~RangeLocator() {
  m_hyperspace_ptr->close(m_root_file_handle);
}



/**
 *
 */
int RangeLocator::find(uint32_t tableId, const char *rowKey, const char **serverIdPtr) {
  int error;

  if (m_root_stale) {
    if ((error = read_root_location()) != Error::OK)
      return error;
  }

  if (m_cache.lookup(tableId, rowKey, serverIdPtr))
    return Error::OK;

  // fix me !!!!

  if (tableId == 0) {
    assert(strcmp(rowKey, "0:"));

    /**
    m_range_server;
    RangeServerClient    
    */
  }

  return Error::OK;
}


/**
 * 
 */
int RangeLocator::read_root_location() {
  int error;
  DynamicBuffer value(0);
  std::string addrStr;
  char *ptr;

  if ((error = m_hyperspace_ptr->attr_get(m_root_file_handle, "location", value)) != Error::OK) {
    LOG_VA_ERROR("Problem reading 'location' attribute of Hyperspace file /hypertable/root - %s", Error::get_text(error));
    return error;
  }

  m_cache.insert(0, 0, "1:", (const char *)value.buf);

  if ((ptr = strchr((const char *)value.buf, '_')) != 0)
    *ptr = 0;

  if (!InetAddr::initialize(&m_root_addr, (const char *)value.buf)) {
    LOG_ERROR("Unable to extract address from /hypertable/root Hyperspace file");
    return Error::BAD_ROOT_SERVERID;
  }

  m_conn_manager_ptr->add(m_root_addr, 30, "Root RangeServer");

  if (!m_conn_manager_ptr->wait_for_connection(m_root_addr, 20)) {
    LOG_VA_ERROR("Timeout (20s) waiting for root RangeServer connection - %s", (const char *)value.buf);
  }

  m_root_stale = false;

  return Error::OK;
}
