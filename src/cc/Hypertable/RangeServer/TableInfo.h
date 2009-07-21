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

#ifndef HYPERTABLE_TABLEINFO_H
#define HYPERTABLE_TABLEINFO_H

#include <map>
#include <string>

#include <boost/thread/mutex.hpp>

#include "Common/StringExt.h"
#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/Types.h"

#include "Range.h"
#include "RangeSet.h"

namespace Hypertable {

  class Schema;

  class TableInfo : public RangeSet {
  public:
    /**
     * Constructor
     *
     * @param master_client smart pointer to master proxy object
     * @param identifier table identifier
     * @param schema smart pointer to schema object
     */
    TableInfo(MasterClientPtr &master_client,
              const TableIdentifier *identifier,
              SchemaPtr &schema);

    virtual bool remove(const String &end_row);
    virtual bool change_end_row(const String &old_end_row,
                                const String &new_end_row);

    /**
     * Returns the table name
     */
    String get_name() { return (String)m_identifier.name; }

    /**
     * Returns the table id
     */
    uint32_t get_id() { return m_identifier.id; }

    /**
     * Returns a pointer to the schema object
     *
     * @return reference to the smart pointer to the schema object
     */
    SchemaPtr &get_schema() {
      ScopedLock lock(m_mutex);
      return m_schema;
    }

    /**
     * Updates the schema object for this entry
     * and propagates the change to all ranges.
     * @param schema_ptr smart pointer to new schema object
     */
    void update_schema(SchemaPtr &schema_ptr);

    /**
     * Returns the range object corresponding to the given range specification
     *
     * @param range_spec range specification
     * @param range reference to smart pointer to range object
     *        (output parameter)
     * @return true if found, false otherwise
     */
    bool get_range(const RangeSpec *range_spec, RangePtr &range);

    /**
     * Removes the range described by the given range spec
     *
     * @param range_spec range specification of range to remove
     * @param range reference to smart pointer to hold removed range
     *        (output parameter)
     * @return true if removed, false if not found
     */
    bool remove_range(const RangeSpec *range_spec, RangePtr &range);

    /**
     * Adds a range
     *
     * @param range smart pointer to range object
     */
    void add_range(RangePtr &range);

    /**
     * Finds the range that the given row belongs to
     *
     * @param row row key used to locate range (in)
     * @param range reference to smart pointer to hold removed range (out)
     * @param start_row starting row of range (out)
     * @param end_row ending row of range (out)
     * @return true if found, false otherwise
     */
    bool find_containing_range(const String &row, RangePtr &range,
                               String &start_row, String &end_row);

    /**
     * Dumps range table information to stdout
     */
    void dump_range_table();

    /**
     * Fills a vector of pointers to range objects
     *
     * @param range_vec smart pointer to range vector
     */
    void get_range_vector(std::vector<RangePtr> &range_vec);

    /**
     * Returns the number of ranges open for this table
     */
    int32_t get_range_count(); 

    /**
     * Clears the range map
     */
    void clear();

    /**
     * Creates a shallow copy of this table info object.  This method
     * copies everything except the range map.
     *
     * @return pointer to the newly created copy of the table info object
     */
    TableInfo *create_shallow_copy();

  private:

    typedef std::map<String, RangePtr> RangeMap;

    Mutex                m_mutex;
    MasterClientPtr      m_master_client;
    TableIdentifierManaged m_identifier;
    SchemaPtr            m_schema;
    RangeMap             m_range_map;
  };

  typedef intrusive_ptr<TableInfo> TableInfoPtr;

} // namespace Hypertable

#endif // HYPERTABLE_TABLEINFO_H
