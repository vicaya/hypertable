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
  TableMutatorSendBufferMapT::const_iterator iter;

  if (!m_cache_ptr->lookup(m_table_identifier.id, key.row, &range_info)) {
    timer.start();
    m_range_locator_ptr->find_loop(&m_table_identifier, key.row, &range_info, timer, false);
  }

  iter = m_buffer_map.find(range_info.location);

  if (iter == m_buffer_map.end()) {
    m_buffer_map[range_info.location] = new TableMutatorSendBuffer(&m_table_identifier, &m_completion_counter, m_range_locator_ptr.get());
    iter = m_buffer_map.find(range_info.location);

    if (!LocationCache::location_to_addr(range_info.location.c_str(), (*iter).second->addr))
      throw Exception(Error::INVALID_METADATA, range_info.location);
  }

  (*iter).second->key_offsets.push_back((*iter).second->accum.fill());
  create_key_and_append((*iter).second->accum, FLAG_INSERT, key.row, key.column_family_code, key.column_qualifier, key.timestamp);
  append_as_byte_string((*iter).second->accum, value, value_len);

  if ((*iter).second->accum.fill() > MAX_SEND_BUFFER_SIZE)
    m_full = true;
}


/**
 *
 */
void TableMutatorScatterBuffer::set_delete(Key &key, Timer &timer) {
  RangeLocationInfo range_info;
  TableMutatorSendBufferMapT::const_iterator iter;

  if (!m_cache_ptr->lookup(m_table_identifier.id, key.row, &range_info)) {
    timer.start();
    m_range_locator_ptr->find_loop(&m_table_identifier, key.row, &range_info, timer, false);
  }

  iter = m_buffer_map.find(range_info.location);

  if (iter == m_buffer_map.end()) {
    m_buffer_map[range_info.location] = new TableMutatorSendBuffer(&m_table_identifier, &m_completion_counter, m_range_locator_ptr.get());
    iter = m_buffer_map.find(range_info.location);

    if (!LocationCache::location_to_addr(range_info.location.c_str(), (*iter).second->addr))
      throw Exception(Error::INVALID_METADATA, range_info.location);
  }

  (*iter).second->key_offsets.push_back((*iter).second->accum.fill());
  uint8_t key_flag;
  if (key.column_family_code == 0)
    key_flag = FLAG_DELETE_ROW;
  else if (key.column_qualifier)
    key_flag = FLAG_DELETE_CELL;
  else
    key_flag = FLAG_DELETE_COLUMN_FAMILY;

  create_key_and_append((*iter).second->accum, key_flag, key.row, key.column_family_code, key.column_qualifier, key.timestamp);
  append_as_byte_string((*iter).second->accum, 0, 0);

  if ((*iter).second->accum.fill() > MAX_SEND_BUFFER_SIZE)
    m_full = true;
}



/**
 *
 */
void TableMutatorScatterBuffer::set(ByteString key, ByteString value, Timer &timer) {
  RangeLocationInfo range_info;
  TableMutatorSendBufferMapT::const_iterator iter;
  uint8_t *ptr = key.ptr;
  size_t len = Serialization::decode_vi32((const uint8_t **)&ptr);

  if (!m_cache_ptr->lookup(m_table_identifier.id, (const char *)ptr, &range_info)) {
    timer.start();
    m_range_locator_ptr->find_loop(&m_table_identifier, (const char *)ptr, &range_info, timer, false);
  }

  iter = m_buffer_map.find(range_info.location);

  if (iter == m_buffer_map.end()) {
    m_buffer_map[range_info.location] = new TableMutatorSendBuffer(&m_table_identifier, &m_completion_counter, m_range_locator_ptr.get());
    iter = m_buffer_map.find(range_info.location);

    if (!LocationCache::location_to_addr(range_info.location.c_str(), (*iter).second->addr))
      throw Exception(Error::INVALID_METADATA, range_info.location);
  }

  (*iter).second->key_offsets.push_back((*iter).second->accum.fill());
  (*iter).second->accum.add(key.ptr, (ptr-key.ptr)+len);
  (*iter).second->accum.add(value.ptr, value.length());

  if ((*iter).second->accum.fill() > MAX_SEND_BUFFER_SIZE)
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
  TableMutatorSendBufferPtr send_buffer_ptr;
  struct ltByteStringChronological swo_bs;
  std::vector<ByteString> kvec;
  uint8_t *ptr;
  ByteString bs;

  m_completion_counter.set(m_buffer_map.size());

  for (TableMutatorSendBufferMapT::const_iterator iter = m_buffer_map.begin(); iter != m_buffer_map.end(); iter++) {
    send_buffer_ptr = (*iter).second;
    kvec.clear();
    kvec.reserve( send_buffer_ptr->key_offsets.size() );
    for (size_t i=0; i<send_buffer_ptr->key_offsets.size(); i++)
      kvec.push_back((ByteString)(send_buffer_ptr->accum.base + send_buffer_ptr->key_offsets[i]));
    sort(kvec.begin(), kvec.end(), swo_bs);
    size_t len = send_buffer_ptr->accum.fill();

    send_buffer_ptr->pending_updates.set(new uint8_t [ len ], len);
    ptr = send_buffer_ptr->pending_updates.base;

    for (size_t i=0; i<kvec.size(); i++) {
      bs = kvec[i];
      bs.next();  // skip key
      bs.next();  // skip value
      memcpy(ptr, kvec[i].ptr, bs.ptr - kvec[i].ptr);
      ptr += bs.ptr - kvec[i].ptr;
    }
    send_buffer_ptr->accum.clear();
    HT_EXPECT((size_t)(ptr-send_buffer_ptr->pending_updates.base)==len, Error::FAILED_EXPECTATION);

    send_buffer_ptr->dispatch_handler_ptr = new TableMutatorDispatchHandler(send_buffer_ptr.get());

    /**
     * Send update
     */
    try {
      send_buffer_ptr->pending_updates.own = false;
      m_range_server.update(send_buffer_ptr->addr, m_table_identifier, send_buffer_ptr->pending_updates, send_buffer_ptr->dispatch_handler_ptr.get());
    }
    catch (Exception &e) {
      send_buffer_ptr->add_retries(0, send_buffer_ptr->pending_updates.size);
      m_completion_counter.decrement();
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
bool TableMutatorScatterBuffer::wait_for_completion(Timer &timer) {
  if (!m_completion_counter.wait_for_completion(timer)) {
    if (m_completion_counter.has_errors()) {
      TableMutatorSendBufferPtr send_buffer_ptr;
      std::vector<FailedRegionT> failed_regions;

      for (TableMutatorSendBufferMapT::const_iterator iter = m_buffer_map.begin(); iter != m_buffer_map.end(); iter++)
	(*iter).second->get_failed_regions(failed_regions);

      if (!failed_regions.empty()) {
	Cell cell;
	Key key;
	ByteString bs;
	uint8_t *endptr;
	Schema::ColumnFamily *cf;
	for (size_t i=0; i<failed_regions.size(); i++) {
	  bs.ptr = failed_regions[i].base;
	  endptr = bs.ptr + failed_regions[i].len;
	  while (bs.ptr < endptr) {
	    key.load(bs);
	    cell.row_key = key.row;
	    cf = m_schema_ptr->get_column_family(key.column_family_code);
	    HT_EXPECT(cf, Error::FAILED_EXPECTATION);
	    cell.column_family = m_constant_strings.get(cf->name.c_str());
	    cell.column_qualifier = key.column_qualifier;
	    cell.timestamp = key.timestamp;
	    bs.next();
	    cell.value_len = bs.decode_length((uint8_t **)&cell.value);
	    bs.next();
	    m_failed_mutations.push_back(std::make_pair(cell, failed_regions[i].error));
	  }
	}
      }

      // this prevents this mutation failure logic from being executed twice
      // if this method gets called again
      m_completion_counter.clear_errors();

      if (m_failed_mutations.size() > 0)
	throw Exception(failed_regions[0].error);

    }
    return false;
  }
  return true;
}


TableMutatorScatterBuffer *TableMutatorScatterBuffer::create_redo_buffer(Timer &timer) {
  TableMutatorSendBufferPtr send_buffer_ptr;
  ByteString key, value, bs;
  TableMutatorScatterBuffer *redo_buffer = 0;

  try {

    redo_buffer = new TableMutatorScatterBuffer(m_props_ptr, m_comm, &m_table_identifier, m_schema_ptr, m_range_locator_ptr);
  
    for (TableMutatorSendBufferMapT::const_iterator iter = m_buffer_map.begin(); iter != m_buffer_map.end(); iter++) {
      send_buffer_ptr = (*iter).second;

      if (send_buffer_ptr->accum.fill()) {
	uint8_t *endptr;

	bs.ptr = send_buffer_ptr->accum.base;
	endptr = bs.ptr + send_buffer_ptr->accum.fill();

	// now add all of the old keys to the redo buffer
	while (bs.ptr < endptr) {
	  key.ptr = bs.next();
	  value.ptr = bs.next();
	  redo_buffer->set(key, value, timer);
	  m_resends++;
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
  for (TableMutatorSendBufferMapT::const_iterator iter = m_buffer_map.begin(); iter != m_buffer_map.end(); iter++)
    (*iter).second->reset();
}
