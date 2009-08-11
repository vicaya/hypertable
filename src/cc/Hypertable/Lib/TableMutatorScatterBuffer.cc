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
#include "Common/Config.h"
#include "Common/Timer.h"
#include "Common/InetAddr.h"

#include "Defaults.h"
#include "Key.h"
#include "KeySpec.h"
#include "TableMutatorDispatchHandler.h"
#include "TableMutatorScatterBuffer.h"
#include "RangeServerProtocol.h"

using namespace Hypertable;


TableMutatorScatterBuffer::TableMutatorScatterBuffer(Comm *comm,
    const TableIdentifier *table_identifier, SchemaPtr &schema,
    RangeLocatorPtr &range_locator, uint32_t timeout_ms)
  : m_comm(comm), m_schema(schema), m_range_locator(range_locator),
    m_range_server(comm, timeout_ms), m_table_identifier(*table_identifier),
    m_full(false), m_resends(0), m_timeout_ms(timeout_ms)  {

  m_loc_cache = m_range_locator->location_cache();

  HT_ASSERT(Config::properties);

  m_server_flush_limit = Config::properties->get_i32(
      "Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer");
  m_refresh_schema = Config::properties->get_bool("Hypertable.Client.RefreshSchema");

}


void
TableMutatorScatterBuffer::set(const Key &key, const void *value,
                               uint32_t value_len, Timer &timer) {
  RangeLocationInfo range_info;
  TableMutatorSendBufferMap::const_iterator iter;

  if (!m_loc_cache->lookup(m_table_identifier.id, key.row, &range_info)) {
    timer.start();
    m_range_locator->find_loop(&m_table_identifier, key.row, &range_info,
                               timer, false);
  }

  iter = m_buffer_map.find(range_info.location);

  if (iter == m_buffer_map.end()) {
    m_buffer_map[range_info.location] = new TableMutatorSendBuffer(
        &m_table_identifier, &m_completion_counter, m_range_locator.get());
    iter = m_buffer_map.find(range_info.location);

    if (!LocationCache::location_to_addr(range_info.location.c_str(),
        (*iter).second->addr))
      HT_THROW(Error::INVALID_METADATA, range_info.location);
  }

  (*iter).second->key_offsets.push_back((*iter).second->accum.fill());
  create_key_and_append((*iter).second->accum, key.flag, key.row,
      key.column_family_code, key.column_qualifier, key.timestamp);
  append_as_byte_string((*iter).second->accum, value, value_len);

  if ((*iter).second->accum.fill() > m_server_flush_limit)
    m_full = true;
}


void TableMutatorScatterBuffer::set_delete(const Key &key, Timer &timer) {
  RangeLocationInfo range_info;
  TableMutatorSendBufferMap::const_iterator iter;

  if (!m_loc_cache->lookup(m_table_identifier.id, key.row, &range_info)) {
    timer.start();
    m_range_locator->find_loop(&m_table_identifier, key.row, &range_info,
                               timer, false);
  }

  iter = m_buffer_map.find(range_info.location);

  if (iter == m_buffer_map.end()) {
    m_buffer_map[range_info.location] = new TableMutatorSendBuffer(
        &m_table_identifier, &m_completion_counter, m_range_locator.get());
    iter = m_buffer_map.find(range_info.location);

    if (!LocationCache::location_to_addr(range_info.location.c_str(),
        (*iter).second->addr))
      HT_THROW(Error::INVALID_METADATA, range_info.location);
  }

  (*iter).second->key_offsets.push_back((*iter).second->accum.fill());
  uint8_t key_flag;
  if (key.column_family_code == 0)
    key_flag = FLAG_DELETE_ROW;
  else if (key.column_qualifier)
    key_flag = FLAG_DELETE_CELL;
  else
    key_flag = FLAG_DELETE_COLUMN_FAMILY;

  create_key_and_append((*iter).second->accum, key_flag, key.row,
      key.column_family_code, key.column_qualifier, key.timestamp);
  append_as_byte_string((*iter).second->accum, 0, 0);

  if ((*iter).second->accum.fill() > m_server_flush_limit)
    m_full = true;
}


void
TableMutatorScatterBuffer::set(SerializedKey key, ByteString value,
                               Timer &timer) {
  RangeLocationInfo range_info;
  TableMutatorSendBufferMap::const_iterator iter;
  const uint8_t *ptr = key.ptr;
  size_t len = Serialization::decode_vi32(&ptr);

  if (!m_loc_cache->lookup(m_table_identifier.id, (const char *)ptr+1,
                           &range_info)) {
    timer.start();
    m_range_locator->find_loop(&m_table_identifier, (const char *)ptr+1,
                               &range_info, timer, false);
  }

  iter = m_buffer_map.find(range_info.location);

  if (iter == m_buffer_map.end()) {
    m_buffer_map[range_info.location] = new TableMutatorSendBuffer(
        &m_table_identifier, &m_completion_counter, m_range_locator.get());
    iter = m_buffer_map.find(range_info.location);

    if (!LocationCache::location_to_addr(range_info.location.c_str(),
        (*iter).second->addr))
      HT_THROW(Error::INVALID_METADATA, range_info.location);
  }

  (*iter).second->key_offsets.push_back((*iter).second->accum.fill());
  (*iter).second->accum.add(key.ptr, (ptr-key.ptr)+len);
  (*iter).second->accum.add(value.ptr, value.length());

  if ((*iter).second->accum.fill() > m_server_flush_limit)
    m_full = true;
}


namespace {

  struct SendRec {
    SerializedKey key;
    uint64_t offset;
  };

  inline bool operator<(const SendRec sr1, const SendRec sr2) {
    const char *row1 = sr1.key.row();
    const char *row2 = sr2.key.row();
    int rval = strcmp(row1, row2);
    if (rval == 0)
      return sr1.offset < sr2.offset;
    return rval < 0;
  }
}


void TableMutatorScatterBuffer::send(RangeServerFlagsMap &rangeserver_flags_map,
                                     uint32_t flags) {
  TableMutatorSendBufferPtr send_buffer;
  std::vector<SendRec> send_vec;
  uint8_t *ptr;
  SerializedKey key;
  SendRec send_rec;
  size_t len;
  String range_location;

  m_completion_counter.set(m_buffer_map.size());

  for (TableMutatorSendBufferMap::const_iterator iter = m_buffer_map.begin();
       iter != m_buffer_map.end(); ++iter) {
    send_buffer = (*iter).second;

    if ((len = send_buffer->accum.fill()) == 0) {
      m_completion_counter.decrement();
      continue;
    }

    send_buffer->pending_updates.set(new uint8_t [len], len);

    if (send_buffer->resend()) {
      memcpy(send_buffer->pending_updates.base,
             send_buffer->accum.base, len);
      send_buffer->send_count = send_buffer->retry_count;
    }
    else {
      send_vec.clear();
      send_vec.reserve(send_buffer->key_offsets.size());
      for (size_t i=0; i<send_buffer->key_offsets.size(); i++) {
        send_rec.key.ptr = send_buffer->accum.base
                           + send_buffer->key_offsets[i];
        send_rec.offset = send_buffer->key_offsets[i];
        send_vec.push_back(send_rec);
      }
      sort(send_vec.begin(), send_vec.end());

      ptr = send_buffer->pending_updates.base;

      for (size_t i=0; i<send_vec.size(); i++) {
        key = send_vec[i].key;
        key.next();  // skip key
        key.next();  // skip value
        memcpy(ptr, send_vec[i].key.ptr, key.ptr - send_vec[i].key.ptr);
        ptr += key.ptr - send_vec[i].key.ptr;
      }
      HT_ASSERT((size_t)(ptr-send_buffer->pending_updates.base)==len);
      send_buffer->dispatch_handler =
          new TableMutatorDispatchHandler(send_buffer.get(), m_refresh_schema);

      send_buffer->send_count = send_buffer->key_offsets.size();
    }

    send_buffer->accum.free();
    send_buffer->key_offsets.clear();

    /**
     * Send update
     */
    try {
      send_buffer->pending_updates.own = false;
      m_range_server.update(send_buffer->addr, m_table_identifier,
          send_buffer->send_count, send_buffer->pending_updates, flags,
          send_buffer->dispatch_handler.get());
      String rs_addr = InetAddr::format(send_buffer->addr);
      rangeserver_flags_map[rs_addr] = flags;
    }
    catch (Exception &e) {
      if (e.code() == Error::COMM_NOT_CONNECTED) {
        send_buffer->add_retries(send_buffer->send_count, 0,
                                 send_buffer->pending_updates.size);
        m_completion_counter.decrement();
      }
    }
    send_buffer->pending_updates.own = true;
  }
}


bool TableMutatorScatterBuffer::completed() {
  return m_completion_counter.is_complete();
}


bool TableMutatorScatterBuffer::wait_for_completion(Timer &timer) {
  if (!m_completion_counter.wait_for_completion(timer)) {
    if (m_completion_counter.has_errors()) {
      TableMutatorSendBufferPtr send_buffer;
      std::vector<FailedRegion> failed_regions;

      for (TableMutatorSendBufferMap::const_iterator it = m_buffer_map.begin();
           it != m_buffer_map.end(); ++it)
        (*it).second->get_failed_regions(failed_regions);

      if (!failed_regions.empty()) {
        Cell cell;
        Key key;
        ByteString bs;
        const uint8_t *endptr;
        Schema::ColumnFamily *cf;
        for (size_t i=0; i<failed_regions.size(); i++) {
          bs.ptr = failed_regions[i].base;
          endptr = bs.ptr + failed_regions[i].len;
          while (bs.ptr < endptr) {
            key.load((SerializedKey)bs);
            cell.row_key = key.row;
            cf = m_schema->get_column_family(key.column_family_code);
            HT_ASSERT(cf);
            cell.column_family = m_constant_strings.get(cf->name.c_str());
            cell.column_qualifier = key.column_qualifier;
            cell.timestamp = key.timestamp;
            bs.next();
            cell.value_len = bs.decode_length(&cell.value);
            bs.next();
            m_failed_mutations.push_back(std::make_pair(cell,
                                         failed_regions[i].error));
          }
        }
      }

      // this prevents this mutation failure logic from being executed twice
      // if this method gets called again
      m_completion_counter.clear_errors();

      if (m_failed_mutations.size() > 0)
        HT_THROW(failed_regions[0].error, "");

    }
    return false;
  }
  return true;
}


TableMutatorScatterBuffer *
TableMutatorScatterBuffer::create_redo_buffer(Timer &timer) {
  TableMutatorSendBufferPtr send_buffer;
  SerializedKey key;
  ByteString value, bs;
  TableMutatorScatterBuffer *redo_buffer = 0;

  try {
    redo_buffer = new TableMutatorScatterBuffer(m_comm, &m_table_identifier,
        m_schema, m_range_locator, m_timeout_ms);

    for (TableMutatorSendBufferMap::const_iterator iter = m_buffer_map.begin();
         iter != m_buffer_map.end(); ++iter) {
      send_buffer = (*iter).second;

      if (send_buffer->accum.fill()) {
        const uint8_t *endptr;

        bs.ptr = send_buffer->accum.base;
        endptr = bs.ptr + send_buffer->accum.fill();

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
  for (TableMutatorSendBufferMap::const_iterator iter = m_buffer_map.begin();
       iter != m_buffer_map.end(); ++iter)
    (*iter).second->reset();
}
