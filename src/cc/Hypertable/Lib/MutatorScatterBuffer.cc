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

namespace {
  const uint32_t MAX_SEND_BUFFER_SIZE = 1000000;
}


/**
 *
 */
MutatorScatterBuffer::MutatorScatterBuffer(Comm *comm, TableIdentifierT *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr) : m_comm(comm), m_schema_ptr(schema_ptr), m_range_locator_ptr(range_locator_ptr), m_range_server(comm, 30), m_table_name(table_identifier->name), m_full(false) {
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

  if ((error = m_range_locator_ptr->find(&m_table_identifier, key.row, &range_info, false)) != Error::OK) {
    if ((error = m_range_locator_ptr->find(&m_table_identifier, key.row, &range_info, 21)) != Error::OK) {
      LOG_VA_ERROR("Unable to locate range server for table %s row %s", m_table_name.c_str(), (const char *)key.row);
      return error;
    }
  }

  iter = m_buffer_map.find(range_info.location);

  if (iter == m_buffer_map.end()) {
    m_buffer_map[range_info.location] = new UpdateBuffer(&m_completion_counter);
    iter = m_buffer_map.find(range_info.location);

    if (!LocationCache::location_to_addr(range_info.location.c_str(), (*iter).second->addr))
      return Error::INVALID_METADATA;
  }

  (*iter).second->key_offsets.push_back((*iter).second->buf.fill());
  CreateKeyAndAppend((*iter).second->buf, FLAG_INSERT, key.row, key.column_family_code, key.column_qualifier, key.timestamp);
  CreateAndAppend((*iter).second->buf, value, value_len);

  if ((*iter).second->buf.fill() > MAX_SEND_BUFFER_SIZE)
    m_full = true;
  
  return Error::OK;
}



/**
 *
 */
int MutatorScatterBuffer::set(ByteString32T *key, ByteString32T *value) {
  int error;
  RangeLocationInfo range_info;
  UpdateBufferMapT::const_iterator iter;

  if ((error = m_range_locator_ptr->find(&m_table_identifier, (const char *)key->data, &range_info, false)) != Error::OK) {
    if ((error = m_range_locator_ptr->find(&m_table_identifier, (const char *)key->data, &range_info, 21)) != Error::OK) {
      LOG_VA_ERROR("Unable to locate range server for table %s row %s", m_table_name.c_str(), (const char *)key->data);
      return error;
    }
  }

  iter = m_buffer_map.find(range_info.location);

  if (iter == m_buffer_map.end()) {
    m_buffer_map[range_info.location] = new UpdateBuffer(&m_completion_counter);
    iter = m_buffer_map.find(range_info.location);

    if (!LocationCache::location_to_addr(range_info.location.c_str(), (*iter).second->addr))
      return Error::INVALID_METADATA;
  }

  (*iter).second->key_offsets.push_back((*iter).second->buf.fill());
  (*iter).second->buf.add(key, Length(key));
  (*iter).second->buf.add(value, Length(value));

  if ((*iter).second->buf.fill() > MAX_SEND_BUFFER_SIZE)
    m_full = true;
  
  return Error::OK;
}

namespace {
  struct ltByteString32Chronological {
    bool operator()(const ByteString32T * bs1ptr, const ByteString32T *bs2ptr) const {
      int rval = strcmp((const char *)bs1ptr->data, (const char *)bs2ptr->data);
      if (rval == 0) {
	const uint8_t *ts1ptr = bs1ptr->data + bs1ptr->len - 8;
	const uint8_t *ts2ptr = bs2ptr->data + bs2ptr->len - 8;
	return memcmp(ts1ptr, ts2ptr, 8) > 0;  // this gives chronological
      }
      return rval < 0;
    }
  };
}


/**
 *
 */
void MutatorScatterBuffer::send() {
  UpdateBufferPtr update_buffer_ptr;
  struct ltByteString32Chronological swo_bs32;
  std::vector<ByteString32T *>  kvec;
  uint8_t *data, *ptr;

  m_completion_counter.set(m_buffer_map.size());

  for (UpdateBufferMapT::const_iterator iter = m_buffer_map.begin(); iter != m_buffer_map.end(); iter++) {
    update_buffer_ptr = (*iter).second;
    if (!update_buffer_ptr->sorted) {
      kvec.clear();
      kvec.reserve( update_buffer_ptr->key_offsets.size() );
      for (size_t i=0; i<update_buffer_ptr->key_offsets.size(); i++)
	kvec.push_back((ByteString32T *)(update_buffer_ptr->buf.buf + update_buffer_ptr->key_offsets[i]));
      sort(kvec.begin(), kvec.end(), swo_bs32);
      uint8_t *src_base, *src_ptr;
      size_t len = update_buffer_ptr->buf.fill();
      ptr = data = new uint8_t [ len ];

      for (size_t i=0; i<kvec.size(); i++) {
	src_ptr = src_base = (uint8_t *)kvec[i];
	src_ptr += Length((const ByteString32T *)src_ptr);  // skip key
	src_ptr += Length((const ByteString32T *)src_ptr);  // skip value
	memcpy(ptr, src_base, src_ptr-src_base);
	ptr += src_ptr - src_base;
      }
    }
    else {
      size_t len = update_buffer_ptr->buf.fill();
      data = new uint8_t [ len ];
      memcpy(data, update_buffer_ptr->buf.buf, len);
      ptr = data + len;
    }

    update_buffer_ptr->dispatch_handler_ptr = new MutatorDispatchHandler(update_buffer_ptr.get());

    /**
     * Send update
     */
    if ((update_buffer_ptr->error = m_range_server.update(update_buffer_ptr->addr, m_table_identifier, data, ptr-data,
							  update_buffer_ptr->dispatch_handler_ptr.get())) != Error::OK)
      update_buffer_ptr->counterp->decrement(update_buffer_ptr->error);
  }
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
int MutatorScatterBuffer::wait_for_completion() {
  return m_completion_counter.wait_for_completion();
}


MutatorScatterBuffer *MutatorScatterBuffer::create_redo_buffer() {
  int error;
  UpdateBufferPtr update_buffer_ptr;
  RangeLocationInfo range_info;
  std::vector<ByteString32T *>  kvec;
  ByteString32T *low_key, *key, *value;
  int count = 0;

  MutatorScatterBuffer *redo_buffer = new MutatorScatterBuffer(m_comm, &m_table_identifier, m_schema_ptr, m_range_locator_ptr);

  for (UpdateBufferMapT::const_iterator iter = m_buffer_map.begin(); iter != m_buffer_map.end(); iter++) {
    update_buffer_ptr = (*iter).second;

    if (update_buffer_ptr->error != Error::OK) {

      if (update_buffer_ptr->error == Error::RANGESERVER_PARTIAL_UPDATE) {
	uint8_t *src_base, *src_ptr, *src_end;

	src_ptr = src_base = update_buffer_ptr->event_ptr->message + 4;
	src_end = update_buffer_ptr->event_ptr->message + update_buffer_ptr->event_ptr->messageLen;

	low_key = (ByteString32T *)src_base;
	
	// do a hard lookup for the lowest key
	if ((error = m_range_locator_ptr->find(&m_table_identifier, (const char *)low_key->data, &range_info, true)) != Error::OK) {
	  LOG_VA_WARN("Unable to locate range server for table %s row %s", m_table_name.c_str(), (const char *)low_key->data);
	  delete redo_buffer;
	  return 0;
	}

	count = 0;

	// now add all of the old keys to the redo buffer
	while (src_ptr < src_end) {
	  key = (ByteString32T *)src_ptr;
	  value = (ByteString32T *)(((uint8_t *)key) + Length(key));
	  if ((error = redo_buffer->set(key, value)) != Error::OK) {
	    LOG_VA_ERROR("Problem adding row '%s' of table '%s' to redo buffer", (const char *)low_key->data, m_table_name.c_str());
	    delete redo_buffer;
	    return 0;
	  }
	  src_ptr += Length(key) + Length(value);
	  count++;
	}

	//LOG_VA_INFO("Partial update, resending %d updates", count);

      }
      else {
	low_key = (ByteString32T *)update_buffer_ptr->buf.buf;

	// find the lowest key
	for (size_t i=0; i<update_buffer_ptr->key_offsets.size(); i++) {
	  key = (ByteString32T *)(update_buffer_ptr->buf.buf + update_buffer_ptr->key_offsets[i]);
	  if (strcmp((char *)key->data, (char *)low_key->data) < 0)
	    low_key = key;
	}

	// do a hard lookup for the lowest key
	if ((error = m_range_locator_ptr->find(&m_table_identifier, (const char *)low_key->data, &range_info, true)) != Error::OK) {
	  LOG_VA_WARN("Unable to locate range server for table %s row %s", m_table_name.c_str(), (const char *)low_key->data);
	  delete redo_buffer;
	  return 0;
	}

	count = 0;

	// now add all of the old keys to the redo buffer
	for (size_t i=0; i<update_buffer_ptr->key_offsets.size(); i++) {
	  key = (ByteString32T *)(update_buffer_ptr->buf.buf + update_buffer_ptr->key_offsets[i]);
	  value = (ByteString32T *)(((uint8_t *)key) + Length(key));
	  if ((error = redo_buffer->set(key, value)) != Error::OK) {
	    LOG_VA_ERROR("Problem adding row '%s' of table '%s' to redo buffer", (const char *)low_key->data, m_table_name.c_str());
	    delete redo_buffer;
	    return 0;
	  }
	  count++;
	}

	LOG_VA_INFO("Send error '%s', resending %d updates", Error::get_text(update_buffer_ptr->error), count);

      }

    }

  }

  return redo_buffer;
}


void MutatorScatterBuffer::reset() {
  for (UpdateBufferMapT::const_iterator iter = m_buffer_map.begin(); iter != m_buffer_map.end(); iter++) {
    (*iter).second->key_offsets.clear();
    (*iter).second->buf.clear();  // maybe deallocate here???
    (*iter).second->sorted = false;
    (*iter).second->error = Error::OK;
    (*iter).second->event_ptr = 0;
    (*iter).second->dispatch_handler_ptr = 0;
  }
}
