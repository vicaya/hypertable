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

#ifndef HYPERTABLE_TABLEMUTATOR_H
#define HYPERTABLE_TABLEMUTATOR_H

#include <iostream>

#include "AsyncComm/ConnectionManager.h"

#include "Common/Properties.h"
#include "Common/StringExt.h"
#include "Common/Timer.h"

#include "Cells.h"
#include "KeySpec.h"
#include "Table.h"
#include "TableMutatorScatterBuffer.h"
#include "RangeLocator.h"
#include "RangeServerClient.h"
#include "Schema.h"
#include "Types.h"

namespace Hypertable {

  /**
   * Provides the ability to mutate a table in the form of adding and deleting
   * rows and cells.  Objects of this class are used to collect mutations and
   * periodically flush them to the appropriate range servers.  There is a 1 MB
   * buffer of mutations for each range server.  When one of the buffers fills
   * up all the buffers are flushed to their respective range servers.
   */
  class TableMutator : public ReferenceCount {

  public:

    /**
     * Constructs the TableMutator object
     *
     * @param props reference to properties smart pointer
     * @param comm pointer to the Comm layer
     * @param table pointer to the table object
     * @param range_locator smart pointer to range locator
     * @param timeout_ms maximum time in milliseconds to allow methods
     *        to execute before throwing an exception
     * @param flags rangeserver client update command flags
     */
    TableMutator(PropertiesPtr &props, Comm *comm, Table *table,
                 RangeLocatorPtr &range_locator, uint32_t timeout_ms,
                 uint32_t flags = 0);

    /**
     * Destructor for TableMutator object
     * Make sure buffers are flushed and unsynced rangeservers get synced.
     */
    virtual ~TableMutator();

    /**
     * Inserts a cell into the table.
     *
     * @param key key of the cell being inserted
     * @param value pointer to the value to store in the cell
     * @param value_len length of data pointed to by value
     */
    virtual void set(const KeySpec &key, const void *value, uint32_t value_len);

    /**
     * Convenient helper for null-terminated values
     */
    void set(const KeySpec &key, const char *value) {
      if (value)
        set(key, value, strlen(value));
      else
        set(key, 0, 0);
    }

    /**
     * Convenient helper for String values
     */
    void set(const KeySpec &key, const String &value) {
      set(key, value.data(), value.length());
    }

    /**
     * Deletes an entire row, a column family in a particular row, or a specific
     * cell within a row.
     *
     * @param key key of the row or cell(s) being deleted
     */
    virtual void set_delete(const KeySpec &key);

    /**
     * Insert a bunch of cells into the table (atomically if cells are in
     * the same range/row)
     *
     * @param cells a list of cells
     */
    virtual void set_cells(const Cells &cells) {
      set_cells(cells.begin(), cells.end());
    }

    /**
     * Flushes the accumulated mutations to their respective range servers.
     */
    virtual void flush();

    /**
     * Retries the last operation
     *
     * @param timeout_ms timeout in milliseconds, 0 means use default timeout
     * @return true if successfully flushed, false otherwise
     */
    virtual bool retry(uint32_t timeout_ms=0);

    /**
     * Returns the amount of memory used by the collected mutations.
     *
     * @return amount of memory used by the collected mutations.
     */
    virtual uint64_t memory_used() { return m_memory_used; }

    /**
     * There are certain circumstances when mutations get flushed to the wrong
     * range server due to stale range location information.  When the correct
     * location information is discovered, these mutations get resent to the
     * proper range server.  This method returns the number of mutations that
     * were resent.
     *
     * @return number of mutations that were resent
     */
    uint64_t get_resend_count() { return m_resends; }

    /**
     * Returns the failed mutations
     *
     * @param failed_mutations reference to vector of Cell/error pairs
     */
    virtual void get_failed(FailedMutations &failed_mutations) {
      if (m_prev_buffer)
        m_prev_buffer->get_failed_mutations(failed_mutations);
    }

    /** Show failed mutations */
    std::ostream &show_failed(const Exception &, std::ostream & = std::cout);

    /**
     * Indicates whether or not there are failed updates to be retried
     *
     * @return true if there are failed updates to retry, false otherwise
     */
    virtual bool need_retry() {
      if (m_prev_buffer)
        return m_prev_buffer->get_failure_count() > 0;
      return false;
    }

    // The flags shd be the same as in Hypertable::RangeServerProtocol.
    enum {
      /* Don't force a commit log sync on update */
      FLAG_NO_LOG_SYNC = 0x0001
    };

  protected:
    void auto_flush(Timer &);

  private:
    enum Operation {
      SET = 1,
      SET_CELLS,
      SET_DELETE,
      FLUSH
    };

    /**
     * Calls sync on any unsynced rangeservers and waits for completion
     */
    void sync();

    void wait_for_previous_buffer(Timer &timer);
    void to_full_key(const void *row, const char *cf, const void *cq,
                     int64_t ts, int64_t rev, uint8_t flag, Key &full_key);
    void to_full_key(const KeySpec &key, Key &full_key) {
      to_full_key(key.row, key.column_family, key.column_qualifier,
                  key.timestamp, key.revision, FLAG_INSERT, full_key);
    }
    void to_full_key(const Cell &cell, Key &full_key) {
      to_full_key(cell.row_key, cell.column_family, cell.column_qualifier,
                  cell.timestamp, cell.revision, cell.flag, full_key);
    }
    void set_cells(Cells::const_iterator start, Cells::const_iterator end);

    void save_last(const KeySpec &key, const void *value, size_t value_len) {
      m_last_key = key;
      m_last_value = value;
      m_last_value_len = value_len;
    }

    void save_last(Cells::const_iterator it, Cells::const_iterator end) {
      m_last_cells_it = it;
      m_last_cells_end = end;
    }

    void handle_exceptions();

    PropertiesPtr        m_props;
    Comm                *m_comm;
    TablePtr             m_table;
    SchemaPtr            m_schema;
    RangeLocatorPtr      m_range_locator;
    TableIdentifierManaged m_table_identifier;
    uint64_t             m_memory_used;
    uint64_t             m_max_memory;
    TableMutatorScatterBufferPtr  m_buffer;
    TableMutatorScatterBufferPtr  m_prev_buffer;
    uint64_t             m_resends;
    uint32_t             m_timeout_ms;
    uint32_t             m_flags;
    uint32_t             m_prev_buffer_flags;
    uint32_t             m_flush_delay;
    RangeServerFlagsMap  m_rangeserver_flags_map;
    int32_t     m_last_error;
    int         m_last_op;
    KeySpec     m_last_key;
    const void *m_last_value;
    uint32_t    m_last_value_len;
    Cells::const_iterator m_last_cells_it;
    Cells::const_iterator m_last_cells_end;
    const static uint32_t ms_max_sync_retries = 5;
    bool       m_refresh_schema;
  };

  typedef intrusive_ptr<TableMutator> TableMutatorPtr;

} // namespace Hypertable

#endif // HYPERTABLE_TABLEMUTATOR_H
