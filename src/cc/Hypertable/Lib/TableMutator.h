/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_TABLEMUTATOR_H
#define HYPERTABLE_TABLEMUTATOR_H

#include "AsyncComm/ConnectionManager.h"

#include "Common/Properties.h"
#include "Common/StringExt.h"
#include "Common/ReferenceCount.h"

#include "Cell.h"
#include "KeySpec.h"
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
   * buffer of mutations for each range server.  When one of the buffers fills up
   * all the buffers are flushed to their respective range servers.
   */
  class TableMutator : public ReferenceCount {

  public:

    /**
     * Constructs the TableMutator object.
     *
     * @param props_ptr smart pointer to configuration properties object
     * @param comm pointer to the Comm layer
     * @param table_identifier pointer to the identifier of the table being mutated
     * @param schema_ptr smart pointer to schema object for table
     * @param range_locator_ptr smart pointer to range locator
     * 
     */
    TableMutator(PropertiesPtr &props_ptr, Comm *comm, TableIdentifierT *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr);

    virtual ~TableMutator() { return; }

    /**
     * Inserts a cell into the table.  
     *
     * NOTE: Use of this method is discouraged.  The system uses timestamps
     * internally to do maintenance and bookkeeping.  Timestamps must be assigned
     * in ascending order and there must be no collisions.  Incorrect use of this
     * method to insert data into a table can result in data loss.
     *
     * @param timestamp timestamp (nanoseconds) of cell
     * @param key key of the cell being inserted
     * @param value pointer to the value to store in the cell
     * @param value_len length of data pointed to by value
     */
    void set(uint64_t timestamp, KeySpec &key, const void *value, uint32_t value_len);

    /**
     * Inserts a cell into the table.  
     *
     * @param key key of the cell being inserted
     * @param value pointer to the value to store in the cell
     * @param value_len length of data pointed to by value
     */
    void set(KeySpec &key, const void *value, uint32_t value_len) {
      set(0, key, value, value_len);
    }

    /**
     * Deletes an entire row, a column family in a particular row, or a specific
     * cell within a row.
     *
     * NOTE: Use of this method is discouraged.  The system uses timestamps
     * internally to do maintenance and bookkeeping.  Timestamps must be assigned
     * in ascending order and there must be no collisions.  Incorrect use of this
     * method to insert data into a table can result in data loss.
     *
     * @param timestamp timestamp (nanoseconds) of cell
     * @param key key of the row or cell(s) being deleted
     */
    void set_delete(uint64_t timestamp, KeySpec &key);

    /**
     * Deletes an entire row, a column family in a particular row, or a specific
     * cell within a row.
     *
     * @param key key of the row or cell(s) being deleted
     */
    void set_delete(KeySpec &key) {
      set_delete(0, key);
    }

    /**
     * Flushes the accumulated mutations to their respective range servers.
     */
    void flush();

    /**
     * Returns the amount of memory used by the collected mutations.
     *
     * @return amount of memory used by the collected mutations.
     */
    uint64_t memory_used();

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

  private:

    void wait_for_previous_buffer();

    void sanity_check_key(KeySpec &key);

    PropertiesPtr        m_props_ptr;
    Comm                *m_comm;
    SchemaPtr            m_schema_ptr;
    RangeLocatorPtr      m_range_locator_ptr;
    std::string          m_table_name;
    TableIdentifierT     m_table_identifier;
    uint64_t             m_memory_used;
    uint64_t             m_max_memory;
    TableMutatorScatterBufferPtr  m_buffer_ptr;
    TableMutatorScatterBufferPtr  m_prev_buffer_ptr;
    uint64_t             m_resends;
  };
  typedef boost::intrusive_ptr<TableMutator> TableMutatorPtr;



}

#endif // HYPERTABLE_TABLEMUTATOR_H
