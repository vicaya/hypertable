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
#include <cstring>

extern "C" {
#include <poll.h>
}

#include <boost/algorithm/string.hpp>

#include "Common/Config.h"
#include "Common/StringExt.h"

#include "Defaults.h"
#include "Key.h"
#include "TableMutator.h"

using namespace Hypertable;
using namespace Hypertable::Config;

void TableMutator::handle_exceptions() {
  try {
    throw;
  }
  catch (Exception &e) {
    m_last_error = e.code();
    HT_ERROR_OUT << e << HT_END;

    if (m_last_error == Error::TABLE_NOT_FOUND
        || m_last_error == Error::RANGESERVER_TABLE_NOT_FOUND)
      m_table->m_not_found = true;
  }
  catch (std::bad_alloc &e) {
    m_last_error = Error::BAD_MEMORY_ALLOCATION;
    HT_ERROR("caught bad_alloc here");
  }
  catch (std::exception &e) {
    m_last_error = Error::EXTERNAL;
    HT_ERRORF("caught std::exception: %s", e.what());
  }
  catch (...) {
    m_last_error = Error::EXTERNAL;
    HT_ERROR("caught unknown exception here");
  }
}


TableMutator::TableMutator(Comm *comm, Table *table, SchemaPtr &schema,
    RangeLocatorPtr &range_locator, uint32_t timeout_ms)
  : m_comm(comm), m_schema(schema), m_range_locator(range_locator),
    m_table(table), m_memory_used(0), m_resends(0),
    m_timeout_ms(timeout_ms), m_flush_delay(0), m_last_error(Error::OK),
    m_last_op(0) {

  HT_ASSERT(timeout_ms);

  if (properties) {
    m_flush_delay = get_i32("Hypertable.Lib.Mutator.FlushDelay");
    m_max_memory = get_i64(
      "Hypertable.Lib.Mutator.ScatterBuffer.FlushLimit.Aggregate");
  }
  m_buffer = new TableMutatorScatterBuffer(m_comm, &m_table->identifier(),
      m_schema, m_range_locator, timeout_ms);
}


void
TableMutator::set(const KeySpec &key, const void *value, uint32_t value_len) {
  Timer timer(m_timeout_ms);

  try {
    m_last_op = SET;
    key.sanity_check();

    Key full_key;
    to_full_key(key, full_key);
    m_buffer->set(full_key, value, value_len, timer);
    m_memory_used += 20 + key.row_len + key.column_qualifier_len + value_len;
    auto_flush(timer);
  }
  catch (...) {
    handle_exceptions();
    save_last(key, value, value_len);
    throw;
  }
}


void
TableMutator::set_cells(Cells::const_iterator it, Cells::const_iterator end) {
  Timer timer(m_timeout_ms);

  try {
    m_last_op = SET_CELLS;

    for (; it != end; ++it) {
      Key full_key;
      const Cell &cell = *it;
      cell.sanity_check();

      if (!cell.column_family) {
        full_key.row = cell.row_key;
        full_key.timestamp = cell.timestamp;
        full_key.revision = cell.revision;
        full_key.flag = cell.flag;
      }
      else
        to_full_key(cell, full_key);

      // assuming all inserts for now
      m_buffer->set(full_key, cell.value, cell.value_len, timer);
      m_memory_used += 20 + strlen(cell.row_key)
          + (cell.column_qualifier ? strlen(cell.column_qualifier) : 0);
    }
    auto_flush(timer);
  }
  catch (...) {
    handle_exceptions();
    save_last(it, end);
    throw;
  }
}


void TableMutator::set_delete(const KeySpec &key) {
  Timer timer(m_timeout_ms);
  Key full_key;

  try {
    m_last_op = SET_DELETE;
    key.sanity_check();

    if (!key.column_family) {
      full_key.row = (const char *)key.row;
      full_key.timestamp = key.timestamp;
      full_key.revision = key.revision;
    }
    else
      to_full_key(key, full_key);

    m_buffer->set_delete(full_key, timer);
    m_memory_used += 20 + key.row_len + key.column_qualifier_len;
    auto_flush(timer);
  }
  catch (...) {
    handle_exceptions();
    m_last_key = key;
    throw;
  }
}


void
TableMutator::to_full_key(const void *row, const char *column_family,
    const void *column_qualifier, int64_t timestamp, int64_t revision,
    uint8_t flag, Key &full_key) {
  if (!column_family)
    HT_THROW(Error::BAD_KEY, "Column family not specified");

  Schema::ColumnFamily *cf = m_schema->get_column_family(column_family);

  if (!cf)
    HT_THROWF(Error::BAD_KEY, "Bad column family '%s'", column_family);

  full_key.row = (const char *)row;
  full_key.column_qualifier = (const char *)column_qualifier;
  full_key.column_family_code = (uint8_t)cf->id;
  full_key.timestamp = timestamp;
  full_key.revision = revision;
  full_key.flag = flag;
}


void TableMutator::auto_flush(Timer &timer) {
  if (m_buffer->full() || m_memory_used > m_max_memory) {
    try {
      m_last_op = FLUSH;
      timer.start();

      if (m_prev_buffer)
        wait_for_previous_buffer(timer);

      if (m_flush_delay)
        poll(0, 0, m_flush_delay);

      m_buffer->send();
      m_prev_buffer = m_buffer;
      m_buffer = new TableMutatorScatterBuffer(m_comm,
          &m_table->identifier(), m_schema, m_range_locator, m_timeout_ms);
      m_memory_used = 0;
    }
    HT_RETHROW("auto flushing")
  }
}


void TableMutator::flush() {
  Timer timer(m_timeout_ms, true);

  try {
    if (m_prev_buffer)
      wait_for_previous_buffer(timer);

    /**
     * If there are buffered updates, send them and wait for completion
     */
    if (m_memory_used > 0) {
      m_buffer->send();
      m_prev_buffer = m_buffer;
      wait_for_previous_buffer(timer);
    }

    m_buffer->reset();
    m_prev_buffer = 0;

  }
  catch (...) {
    handle_exceptions();
    m_last_op = FLUSH;
    throw;
  }
}


bool TableMutator::retry(uint32_t timeout_ms) {
  uint32_t save_timeout = m_timeout_ms;

  if (m_last_error == Error::OK)
    return true;

  m_last_error = Error::OK;

  try {
    if (timeout_ms != 0)
      m_timeout_ms = timeout_ms;

    switch (m_last_op) {
    case SET:        set(m_last_key, m_last_value, m_last_value_len);   break;
    case SET_DELETE: set_delete(m_last_key);                            break;
    case SET_CELLS:  set_cells(m_last_cells_it, m_last_cells_end);      break;
    case FLUSH:      flush();                                           break;
    }
  }
  catch(...) {
    m_timeout_ms = save_timeout;
    return false;
  }
  m_timeout_ms = save_timeout;
  return true;
}


void TableMutator::wait_for_previous_buffer(Timer &timer) {
  try {
    TableMutatorScatterBuffer *redo_buffer = 0;
    uint32_t wait_time = 1000;

    timer.start();

    while (!m_prev_buffer->wait_for_completion(timer)) {

      if (timer.remaining() < wait_time)
        HT_THROW_(Error::REQUEST_TIMEOUT);

      // wait a bit
      poll(0, 0, wait_time);
      wait_time += 2000;

      // redo buffer is needed to resend (ranges split/moves etc)
      if ((redo_buffer = m_prev_buffer->create_redo_buffer(timer)) == 0)
        continue;

      m_resends += m_prev_buffer->get_resend_count();
      m_prev_buffer = redo_buffer;

      // Re-send failed updates
      m_prev_buffer->send();
    }
  }
  HT_RETHROW("waiting for previous buffer")
}


std::ostream &
TableMutator::show_failed(const Exception &e, std::ostream &out) {
  FailedMutations failed_mutations;

  get_failed(failed_mutations);

  if (!failed_mutations.empty()) {
    foreach(const FailedMutation &v, failed_mutations) {
      out << "Failed: (" << v.first.row_key << "," << v.first.column_family;

      if (v.first.column_qualifier && *(v.first.column_qualifier))
        out << ":" << v.first.column_qualifier;

      out << "," << v.first.timestamp << ") - "
          << Error::get_text(v.second) << '\n';
    }
    out.flush();
  }
  else throw e;

  return out;
}
