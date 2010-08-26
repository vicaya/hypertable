/** -*- c++ -*-
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
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

#ifndef HYPERTABLE_NAMESPACE_H
#define HYPERTABLE_NAMESPACE_H

#include <boost/tokenizer.hpp>

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/String.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/ConnectionManager.h"
#include "Hyperspace/Session.h"

#include "MasterClient.h"
#include "NameIdMapper.h"
#include "TableCache.h"
#include "Table.h"
#include "TableScanner.h"
#include "TableSplit.h"
#include "TableMutator.h"
#include "NamespaceListing.h"

namespace Hypertable {

  class Comm;
  class HqlInterpreter;

  class Namespace: public ReferenceCount {
  public:

    /**
     * Constructs the object
     *
     */
    Namespace(const String &name, const String &id, PropertiesPtr &props,
              ConnectionManagerPtr &conn_manager, Hyperspace::SessionPtr &hyperspace,
              ApplicationQueuePtr &app_queue, NameIdMapperPtr &namemap,
              MasterClientPtr &master_client, RangeLocatorPtr &range_locator,
              TableCachePtr &table_cache, uint32_t timeout);

    ~Namespace() {}

    /**
     * Get canonical format of name/id string. This means no leading or trailing '/' and
     * cleanup '//'
     */
    static void canonicalize(String *original);

    String get_name() const {
      String dstr = m_name;
      return dstr;
    }

    String get_id() const {
      String dstr = m_id;
      return dstr;
    }

    /**
     * Creates a table
     *
     * The schema parameter is a string that contains an XML-style
     * schema specification.  The best way to learn the syntax
     * of this specification format is to create tables with HQL
     * in the command interpreter and then run DESCRIBE TABLE to
     * see what the XML specification looks like.  For example,
     * the following HQL:
     *
     * <pre>
     * CREATE TABLE COMPRESSOR="lzo" foo (
     *   a MAX_VERSIONS=1 TTL=30 DAYS,
     *   b TTL=1 WEEKS,
     *   c,
     *   ACCESS GROUP primary IN_MEMORY BLOCKSIZE=1024( a ),
     *   ACCESS GROUP secondary COMPRESSOR="zlib" BLOOMFILTER="none" ( b, c)
     * );
     *</pre>
     *
     * will create a table with the follwing XML schema:
     *
     * <pre>
     * <Schema compressor="lzo">
     *   <AccessGroup name="primary" inMemory="true" blksz="1024">
     *     <ColumnFamily>
     *       <Name>a</Name>
     *       <MaxVersions>1</MaxVersions>
     *       <ttl>2592000</ttl>
     *     </ColumnFamily>
     *   </AccessGroup>
     *   <AccessGroup name="secondary" compressor="zlib" bloomFilter="none">
     *     <ColumnFamily>
     *       <Name>b</Name>
     *       <ttl>604800</ttl>
     *     </ColumnFamily>
     *     <ColumnFamily>
     *       <Name>c</Name>
     *     </ColumnFamily>
     *   </AccessGroup>
     * </Schema>
     * </pre>
     *
     * @param name name of the table
     * @param schema schema definition for the table
     */
    void create_table(const String &name, const String &schema);

    /**
     * Alters column families within a table.  The schema parameter
     * contains an XML-style schema difference and supports a
     * 'deleted' element within the 'ColumnFamily' element which
     * indicates that the column family should be deleted.  For example,
     * the following XML diff:
     *
     * <pre>
     * <Schema>
     *   <AccessGroup name="secondary">
     *     <ColumnFamily>
     *       <Name>b</Name>
     *       <deleted>true</deleted>
     *     </ColumnFamily>
     *   </AccessGroup>
     *   <AccessGroup name="tertiary" blksz="2048">
     *     <ColumnFamily>
     *       <Name>d</Name>
     *       <MaxVersions>5</MaxVersions>
     *     </ColumnFamily>
     *   </AccessGroup>
     * </Schema>
     * </pre>
     *
     * when applied to the 'foo' table, described in the create_table
     * example, generates a table that is equivalent to one created
     * with the following HQL:
     *
     * <pre>
     * CREATE TABLE COMPRESSOR="lzo" foo (
     *   a MAX_VERSIONS=1 TTL=2592000,
     *   c,
     *   d MAX_VERSIONS=5,
     *   ACCESS GROUP primary IN_MEMORY BLOCKSIZE=1024 (a),
     *   ACCESS GROUP secondary COMPRESSOR="zlib" BLOOMFILTER="none" (b, c),
     *   ACCESS GROUP tertiary BLOCKSIZE=2048 (d)
     * );
     * </pre>
     *
     * @param name name of the table
     * @param schema desired alterations represented as schema
     */
    void alter_table(const String &name, const String &schema);

    /**
     * Opens a table
     *
     * @param name name of the table
     * @param force by pass any cache if possible
     * @return pointer to Table object
     */
    TablePtr open_table(const String &name, bool force = false);

    /**
     * Refreshes the cached table entry
     *
     * @param name name of the table
     */
    void refresh_table(const String &name);

    /**
     * Checks if the table exists
     *
     * @param name name of table
     * @return true of table exists false ow
     */
    bool exists_table(const String &name);

    /**
     * Returns the table identifier for a table
     *
     * @param name name of table
     * @return identifier string for the table
     */
    String get_table_id(const String &name);

    /**
     * Returns a smart ptr to a schema object for a table
     *
     * @param name table name
     * @return schema object of table
     */
    SchemaPtr get_schema(const String &name);

    /**
     * Returns the schema for a table
     *
     * @param name table name
     * @param with_ids include generation and column family ID attributes
     * @return XML schema of table
     */
    String get_schema_str(const String &name, bool with_ids=false);

    /**
     * Returns a list of existing tables &  namesspaces
     *
     * @param tables reference to vector of table names
     */
    void get_listing(std::vector<NamespaceListing> &listing);

    /**
     * Renames a table.
     *
     * @param old_name old table name
     * @param new_name new table name
     */
    void rename_table(const String &old_name, const String &new_name);

    /**
     * Removes a table.  This command instructs the Master to
     * remove a table from the system, including all of its
     * ranges.
     *
     * @param name table name
     * @param if_exists don't throw an exception if table does not exist
     */
    void drop_table(const String &name, bool if_exists);

    /**
     * Returns a list of existing table names
     *
     * @param name table name
     * @param splits reference to TableSplitsContainer object
     */
    void get_table_splits(const String &name, TableSplitsContainer &splits);

  private:
    typedef hash_map<String, TablePtr> TableCache;
    typedef boost::tokenizer<boost::char_separator<char> > Tokenizer;
    String get_full_name(const String &sub_name);

    void initialize();
    TablePtr _open_table(const String &full_name, bool force=false);

    String                  m_name;
    String                  m_id;
    PropertiesPtr           m_props;
    Comm                   *m_comm;
    ConnectionManagerPtr    m_conn_manager;
    Hyperspace::SessionPtr  m_hyperspace;
    ApplicationQueuePtr     m_app_queue;
    NameIdMapperPtr         m_namemap;
    MasterClientPtr         m_master_client;
    RangeLocatorPtr         m_range_locator;
    String                  m_toplevel_dir;
    bool                    m_hyperspace_reconnect;
    bool                    m_refresh_schema;
    Mutex                   m_mutex;
    TableCachePtr           m_table_cache;
    uint32_t                m_timeout_ms;
  };

  typedef intrusive_ptr<Namespace> NamespacePtr;

} // namespace Hypertable

#endif // HYPERTABLE_NAMESPACE_H
