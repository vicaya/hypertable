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

#include "Common/Timer.h"

#include "Defaults.h"
#include "Key.h"
#include "KeySpec.h"
#include "TableMutatorDispatchHandler.h"
#include "TableMutatorScatterBuffer.h"

using namespace Hypertable;

namespace {
  const uint32_t MAX_SEND_BUFFER_SIZE = 1000000;
}


/**
 *
 */
TableMutatorScatterBuffer::TableMutatorScatterBuffer(PropertiesPtr &props_ptr, Comm *comm, TableIdentifier *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr) : m_props_ptr(props_ptr), m_comm(comm), m_schema_ptr(schema_ptr), m_range_locator_ptr(range_locator_ptr), m_range_server(comm, HYPERTABLE_CLIENT_TIMEOUT), m_table_identifier(table_identifier), m_full(false), m_resends(0) {

  m_range_locator_ptr->get_location_cache(m_cache_ptr);
}



/**
 *
 */
void TableMutatorScatterBuffer::set(Key &key, const void *value, uint32_t value_len, Timer &timer) {
  RangeLocationInfo range_info;
  UpdateBufferMapT::const_iterator iter;

  if (!m_cache_ptr->lookup(m_table_identifier.id, key.row, &range_info)) {
    timer.start();
    m_range_locator_ptr->find_loop(&m_table_identifier, key.row, &range_info, timer, false);
  }

  iter = m_buffer_map.find(range_info.location);

  if (iter == m_buffer_map.end()) {
    m_buffer_map[range_info.location] = new UpdateBuffer(&m_completion_counter);
    iter = m_buffer_map.find(range_info.location);

    if (!LocationCache::location_to_addr(range_info.location.c_str(), (*iter).second->addr))
      throw Exception(Error::INVALID_METADATA, range_info.location);
  }

  (*iter).second->key_offsets.push_back((*iter).second->buf.fill());
  create_key_and_append((*iter).second->buf, FLAG_INSERT, key.row, key.column_family_code, key.column_qualifier, key.timestamp);
  append_as_byte_string((*iter).second->buf, value, value_len);

  if ((*iter).second->buf.fill() > MAX_SEND_BUFFER_SIZE)
    m_full = true;
}


/**
 *
 */
void TableMutatorScatterBuffer::set_delete(Key &key, Timer &timer) {
  RangeLocationInfo range_info;
  UpdateBufferMapT::const_iterator iter;

  if (!m_cache_ptr->lookup(m_table_identifier.id, key.row, &range_info)) {
    timer.start();
    m_range_locator_ptr->find_loop(&m_table_identifier, key.row, &range_info, timer, false);
  }

  iter = m_buffer_map.find(range_info.location);

  if (iter == m_buffer_map.end()) {
    m_buffer_map[range_info.location] = new UpdateBuffer(&m_completion_counter);
    iter = m_buffer_map.find(range_info.location);

    if (!LocationCache::location_to_addr(range_info.location.c_str(), (*iter).second->addr))
      throw Exception(Error::INVALID_METADATA, range_info.location);
  }

  (*iter).second->key_offsets.push_back((*iter).second->buf.fill());
  uint8_t key_flag;
  if (key.column_family_code == 0)
    key_flag = FLAG_DELETE_ROW;
  else if (key.column_qualifier)
    key_flag = FLAG_DELETE_CELL;
  else
    key_flag = FLAG_DELETE_COLUMN_FAMILY;

  create_key_and_append((*iter).second->buf, key_flag, key.row, key.column_family_code, key.column_qualifier, key.timestamp);
  append_as_byte_string((*iter).second->buf, 0, 0);

  if ((*iter).second->buf.fill() > MAX_SEND_BUFFER_SIZE)
    m_full = true;
}



/**
 *
 */
void TableMutatorScatterBuffer::set(ByteString key, ByteString value, Timer &timer) {
  RangeLocationInfo range_info;
  UpdateBufferMapT::const_iterator iter;
  uint8_t *ptr = key.ptr;
  size_t len = Serialization::decode_vi32((const uint8_t **)&ptr);

  if (!m_cache_ptr->lookup(m_table_identifier.id, (const char *)ptr, &range_info)) {
    timer.start();
    m_range_locator_ptr->find_loop(&m_table_identifier, (const char *)ptr, &range_info, timer, false);
  }

  iter = m_buffer_map.find(range_info.location);

  if (iter == m_buffer_map.end()) {
    m_buffer_map[range_info.location] = new UpdateBuffer(&m_completion_counter);
    iter = m_buffer_map.find(range_info.location);

    if (!LocationCache::location_to_addr(range_info.location.c_str(), (*iter).second->addr))
      throw Exception(Error::INVALID_METADATA, range_info.location);
  }

  (*iter).second->key_offsets.push_back((*iter).second->buf.fill());
  (*iter).second->buf.add(key.ptr, (ptr-key.ptr)+len);
  (*iter).second->buf.add(value.ptr, value.length());

  if ((*iter).second->buf.fill() > MAX_SEND_BUFFER_SIZE)
    m_full = true;
}


namespace {
  struct ltByteStringChronological {
    bool operator()(const ByteString bs1, const ByteString bs2) const {
      uint8_t *ptr1, *ptr2;
      size_t len1 = bs1.decode_length(&ptr1);
      size_t len2 = bs2.decode_length(&ptr2);
      int rval = strcmp((const char *)ptr1, (const char *)ptr2);
      if (rval == 0) {
	ptr1 += len1 - 8;
	ptr2 += len2 - 8;
	return memcmp(ptr1, ptr2, 8) > 0;  // this gives chronological
      }
      return rval < 0;
    }
  };
}


/**
 *
 */
void TableMutatorScatterBuffer::send() {
  UpdateBufferPtr update_buffer_ptr;
  struct ltByteStringChronological swo_bs;
  std::vector<ByteString> kvec;
  uint8_t *data, *ptr;
  ByteString bs;

  m_completion_counter.set(m_buffer_map.size());

  for (UpdateBufferMapT::const_iterator iter = m_buffer_map.begin(); iter != m_buffer_map.end(); iter++) {
    update_buffer_ptr = (*iter).second;
    if (!update_buffer_ptr->sorted) {
      kvec.clear();
      kvec.reserve( update_buffer_ptr->key_offsets.size() );
      for (size_t i=0; i<update_buffer_ptr->key_offsets.size(); i++)
	kvec.push_back((ByteString)(update_buffer_ptr->buf.base + update_buffer_ptr->key_offsets[i]));
      sort(kvec.begin(), kvec.end(), swo_bs);
      size_t len = update_buffer_ptr->buf.fill();
      ptr = data = new uint8_t [ len ];

      for (size_t i=0; i<kvec.size(); i++) {
	bs = kvec[i];
	bs.next();  // skip key
	bs.next();  // skip value
	memcpy(ptr, kvec[i].ptr, bs.ptr - kvec[i].ptr);
	ptr += bs.ptr - kvec[i].ptr;
      }
    }
    else {
      size_t len = update_buffer_ptr->buf.fill();
      data = new uint8_t [ len ];
      memcpy(data, update_buffer_ptr->buf.base, len);
      ptr = data + len;
    }

    update_buffer_ptr->dispatch_handler_ptr = new TableMutatorDispatchHandler(update_buffer_ptr.get());

    /**
     * Send update
     */
    try {
      StaticBuffer buf(data, ptr-data);
      m_range_server.update(update_buffer_ptr->addr, m_table_identifier, buf, update_buffer_ptr->dispatch_handler_ptr.get());
      update_buffer_ptr->error = Error::OK;
    }
    catch (Exception &e) {
      update_buffer_ptr->error = e.code();
      update_buffer_ptr->counterp->decrement(update_buffer_ptr->error);
    }
  }
}



/**
 * 
 */
bool TableMutatorScatterBuffer::completed() {
  return m_completion_counter.is_complete();
}



/**
 * 
 */
int TableMutatorScatterBuffer::wait_for_completion(Timer &timer) {
  return m_completion_counter.wait_for_completion(timer);
}


TableMutatorScatterBuffer *TableMutatorScatterBuffer::create_redo_buffer(Timer &timer) {
  UpdateBufferPtr update_buffer_ptr;
  RangeLocationInfo range_info;
  std::vector<ByteString>  kvec;
  ByteString key, value, bs;
  int count = 0;
  TableMutatorScatterBuffer *redo_buffer = 0;

  try {

    redo_buffer = new TableMutatorScatterBuffer(m_props_ptr, m_comm, &m_table_identifier, m_schema_ptr, m_range_locator_ptr);
  
    for (UpdateBufferMapT::const_iterator iter = m_buffer_map.begin(); iter != m_buffer_map.end(); iter++) {
      update_buffer_ptr = (*iter).second;

      if (update_buffer_ptr->error != Error::OK) {

	if (update_buffer_ptr->error == Error::RANGESERVER_PARTIAL_UPDATE) {
	  uint8_t *ptr, *src_end;

	  bs.ptr = ptr = update_buffer_ptr->event_ptr->message + 4;
	  src_end = update_buffer_ptr->event_ptr->message + update_buffer_ptr->event_ptr->messageLen;

	  // do a hard lookup for the lowest key
	  Serialization::decode_vi32((const uint8_t **)&ptr);
	  m_range_locator_ptr->find_loop(&m_table_identifier, (const char *)ptr, &range_info, timer, true);

	  // now add all of the old keys to the redo buffer
	  while (bs.ptr < src_end) {
	    key.ptr = bs.next();
	    value.ptr = bs.next();
	    redo_buffer->set(key, value, timer);
	    m_resends++;
	  }
	  //HT_INFOF("Partial update, resending %d updates", count);
	}
	else {
	  const char *low_row;
	  size_t len;
	  uint8_t *ptr;

	  bs.ptr = update_buffer_ptr->buf.base;
	  len = bs.decode_length(&ptr);
	  low_row = (const char *)ptr;

	  // find the lowest key
	  for (size_t i=0; i<update_buffer_ptr->key_offsets.size(); i++) {
	    bs.ptr = update_buffer_ptr->buf.base + update_buffer_ptr->key_offsets[i];
	    len = key.decode_length(&ptr);
	    if (strcmp((const char *)ptr, low_row) < 0)
	      low_row = (const char *)ptr;
	  }

	  // do a hard lookup for the lowest key
	  m_range_locator_ptr->find_loop(&m_table_identifier, low_row, &range_info, timer, true);

	  count = 0;

	  // now add all of the old keys to the redo buffer
	  for (size_t i=0; i<update_buffer_ptr->key_offsets.size(); i++) {
	    key.ptr = update_buffer_ptr->buf.base + update_buffer_ptr->key_offsets[i];
	    value.ptr = key.ptr + key.length();
	    redo_buffer->set(key, value, timer);
	    count++;
	  }

	  HT_INFOF("Send error '%s', resending %d updates", Error::get_text(update_buffer_ptr->error), count);

	}

      }
    }
  }
  catch (Exception &e) {
    delete redo_buffer;
    throw;
  }

  return redo_buffer;
}


void TableMutatorScatterBuffer::reset() {
  for (UpdateBufferMapT::const_iterator iter = m_buffer_map.begin(); iter != m_buffer_map.end(); iter++) {
    (*iter).second->key_offsets.clear();
    (*iter).second->buf.clear();  // maybe deallocate here???
    (*iter).second->sorted = false;
    (*iter).second->error = Error::OK;
    (*iter).second->event_ptr = 0;
    (*iter).second->dispatch_handler_ptr = 0;
  }
}
