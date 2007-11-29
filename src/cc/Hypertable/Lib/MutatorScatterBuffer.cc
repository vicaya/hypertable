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

#include "Key.h"
#include "KeySpec.h"
#include "MutatorDispatchHandler.h"
#include "MutatorScatterBuffer.h"


/**
 *
 */
MutatorScatterBuffer::MutatorScatterBuffer(ConnectionManagerPtr &conn_manager_ptr, TableIdentifierT *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr) : m_conn_manager_ptr(conn_manager_ptr), m_schema_ptr(schema_ptr), m_range_locator_ptr(range_locator_ptr), m_range_server(conn_manager_ptr->get_comm(), 30), m_table_name(table_identifier->name) {
  // copy TableIdentifierT
  memcpy(&m_table_identifier, table_identifier, sizeof(TableIdentifierT));
  m_table_identifier.name = m_table_name.c_str();
}


/**
 *
 */
int MutatorScatterBuffer::set(Key &key, uint8_t *value, uint32_t value_len) {
  int error;
  RangeLocationInfo range_info;
  UpdateBufferMapT::const_iterator iter;

  if ((error = m_range_locator_ptr->find(&m_table_identifier, key.row, &range_info)) != Error::OK)
    return error;

  iter = m_buffer_map.find(range_info.location);

  if (iter == m_buffer_map.end()) {
    // TODO:  fix me!
    m_buffer_map[range_info.location] = new UpdateBuffer(&m_completion_counter);
    iter = m_buffer_map.find(range_info.location);

    if (!LocationCache::location_to_addr(range_info.location.c_str(), (*iter).second->addr))
      return Error::INVALID_METADATA;
  }

  (*iter).second->keys.push_back((const ByteString32T *)(*iter).second->buf.ptr);
  CreateKeyAndAppend((*iter).second->buf, FLAG_INSERT, key.row, key.column_family_code, key.column_qualifier, key.timestamp);
  CreateAndAppend((*iter).second->buf, value, value_len);
  
  return Error::OK;
}



/**
 *
 */
int MutatorScatterBuffer::send() {
  UpdateBufferPtr update_buffer_ptr;
  struct ltByteString32 swo_bs32;

  update_buffer_ptr->counterp->set(m_buffer_map.size());

  for (UpdateBufferMapT::const_iterator iter = m_buffer_map.begin(); iter != m_buffer_map.end(); iter++) {
    update_buffer_ptr = (*iter).second;
    sort(update_buffer_ptr->keys.begin(), update_buffer_ptr->keys.begin(), swo_bs32);
    uint8_t *data, *ptr, *src_base, *src_ptr;
    size_t len = update_buffer_ptr->buf.fill();
    ptr = data = new uint8_t [ len ];

    for (size_t i=0; i<update_buffer_ptr->keys.size(); i++) {
      src_ptr = src_base = (uint8_t *)update_buffer_ptr->keys[i];
      src_ptr += Length((const ByteString32T *)src_ptr);  // skip key
      src_ptr += Length((const ByteString32T *)src_ptr);  // skip value
      memcpy(ptr, src_base, src_ptr-src_base);
      ptr += src_ptr - src_base;
    }

    update_buffer_ptr->dispatch_handler_ptr = new MutatorDispatchHandler(update_buffer_ptr.get());

    /**
     * Send update
     */
    if ((update_buffer_ptr->error = m_range_server.update(update_buffer_ptr->addr, m_table_identifier, data, ptr-data,
							  update_buffer_ptr->dispatch_handler_ptr.get())) != Error::OK)
      update_buffer_ptr->counterp->decrement(true);
  }
  
  return false;
}



/**
 * 
 */
bool MutatorScatterBuffer::completed() {
  return m_completion_counter.is_complete();
}



/**
 * 
 */
bool MutatorScatterBuffer::wait_for_completion() {
  return m_completion_counter.wait_for_completion();
}


