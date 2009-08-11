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
#include "TableMutatorSyncDispatchHandler.h"

using namespace Hypertable;
using namespace Hypertable::Config;

void TableMutator::handle_exceptions() {
  try {
    throw;
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


TableMutator::TableMutator(PropertiesPtr & props, Comm *comm, Table *table,
    RangeLocatorPtr &range_locator, uint32_t timeout_ms, uint32_t flags)
  : m_comm(comm), m_table(table), m_range_locator(range_locator),
    m_memory_used(0), m_resends(0), m_timeout_ms(timeout_ms), m_flags(flags),
    m_prev_buffer_flags(0), m_flush_delay(0), m_last_error(Error::OK),
    m_last_op(0) {

  HT_ASSERT(timeout_ms);

  table->get(m_table_identifier, m_schema);

  m_flush_delay = props->get_i32("Hypertable.Mutator.FlushDelay");
  m_max_memory = props->get_i64("Hypertable.Mutator.ScatterBuffer.FlushLimit.Aggregate");
  m_refresh_schema = props->get_bool("Hypertable.Client.RefreshSchema");
  m_buffer = new TableMutatorScatterBuffer(m_comm, &m_table_identifier,
      m_schema, m_range_locator, timeout_ms);
}

TableMutator::~TableMutator() {
  // Flush buffers and sync rangeserver commit logs
  HT_TRY_OR_LOG("final flush", flush());
}

void
TableMutator::set(const KeySpec &key, const void *value, uint32_t value_len) {
  Timer timer(m_timeout_ms);

  try {
    m_last_op = SET;
    auto_flush(timer);
    key.sanity_check();

    Key full_key;
    to_full_key(key, full_key);
    m_buffer->set(full_key, value, value_len, timer);
    m_memory_used += 20 + key.row_len + key.column_qualifier_len + value_len;
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
    auto_flush(timer);

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
    auto_flush(timer);
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

  if (!cf) {
    if (m_refresh_schema) {
      m_table->refresh(m_table_identifier, m_schema);
      m_buffer->refresh_schema(m_table_identifier, m_schema);
      cf = m_schema->get_column_family(column_family);
      if (!cf)
        HT_THROWF(Error::BAD_KEY, "Bad column family '%s'", column_family);
    }
    else
      HT_THROWF(Error::BAD_KEY, "Bad column family '%s'", column_family);
  }

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

      m_buffer->send(m_rangeserver_flags_map, m_flags);
      m_prev_buffer = m_buffer;
      m_prev_buffer_flags = m_flags;
      m_buffer = new TableMutatorScatterBuffer(m_comm, &m_table_identifier,
          m_schema, m_range_locator, m_timeout_ms);
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
     * Wait for commit log sync
     */
    if (m_memory_used > 0) {
      // flush & sync non-empty buffers
      m_buffer->send(m_rangeserver_flags_map, 0);
      // sync remaining unsynced rangeservers
      sync();
      m_prev_buffer = m_buffer;
      m_prev_buffer_flags = 0;
      wait_for_previous_buffer(timer);
      m_rangeserver_flags_map.clear();
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

void TableMutator::sync() {
  vector<String> unsynced_rangeservers;

  try {
    for (hash_map<String, uint32_t>::iterator iter = m_rangeserver_flags_map.begin();
         iter != m_rangeserver_flags_map.end(); ++iter) {
      if ((iter->second & FLAG_NO_LOG_SYNC) == FLAG_NO_LOG_SYNC)
        unsynced_rangeservers.push_back(iter->first);
    }

    if (!unsynced_rangeservers.empty()) {
      TableMutatorSyncDispatchHandler sync_handler(m_comm, 5000);

      for(vector<String>::iterator iter = unsynced_rangeservers.begin();
          iter != unsynced_rangeservers.end(); ++iter ) {
        sync_handler.add((*iter));
      }

      if (!sync_handler.wait_for_completion()) {
        std::vector<TableMutatorSyncDispatchHandler::ErrorResult> errors;
        uint32_t retry_count = 0;
        bool retry_failed;
        do {
          retry_count++;
          sync_handler.get_errors(errors);
          for (size_t i=0; i<errors.size(); i++) {
            HT_ERRORF("commit log sync error - %s - %s",
                errors[i].msg.c_str(), Error::get_text(errors[i].error));
          }
          sync_handler.retry();
        }
        while ((retry_failed = (!sync_handler.wait_for_completion())) &&
            retry_count < ms_max_sync_retries);
        /**
         * Commit log sync failed
         */
        if (retry_failed) {
          sync_handler.get_errors(errors);
          String error_str;
          error_str =  (String) "commit log sync error '" + errors[0].msg.c_str() + "' '" +
                       Error::get_text(errors[0].error) + "' max retry limit=" +
                       ms_max_sync_retries + " hit";
          HT_THROW(errors[0].error, error_str);
        }
      }
    }
  }
  catch (...) {
    handle_exceptions();
    throw;
  }
}

void TableMutator::wait_for_previous_buffer(Timer &timer) {
  TableMutatorScatterBuffer *redo_buffer = 0;
  uint32_t wait_time = 1000;
  timer.start();

  try_again:
  try {
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
      m_prev_buffer->send(m_rangeserver_flags_map, m_prev_buffer_flags);
    }
  }
  catch (Exception &e) {
    m_last_error = e.code();

    if (m_refresh_schema && m_last_error == Error::RANGESERVER_GENERATION_MISMATCH) {
      m_table->refresh(m_table_identifier, m_schema);
      m_prev_buffer->refresh_schema(m_table_identifier, m_schema);
      // redo buffer is needed to resend (ranges split/moves etc)
      if ((redo_buffer = m_prev_buffer->create_redo_buffer(timer)) != 0) {
        m_resends += m_prev_buffer->get_resend_count();
        m_prev_buffer = redo_buffer;
        // Re-send failed updates
        m_prev_buffer->send(m_rangeserver_flags_map, m_prev_buffer_flags);
        goto try_again;
      }
    }
    else
      HT_THROW(e.code(), e.what());
  }
  catch (std::bad_alloc &e) {
    HT_THROW(Error::BAD_MEMORY_ALLOCATION, "waiting for previous buffer");
  }
  catch (std::exception &e) {
    HT_THROW(Error::EXTERNAL, "caught std::exception: waiting for previous buffer ");
  }
  catch (...) {
    HT_ERROR("caught unknown exception ");
    throw;
  }

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
