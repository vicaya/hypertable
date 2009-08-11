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

#ifndef HYPERTABLE_CLIENT_H
#define HYPERTABLE_CLIENT_H

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/String.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/ConnectionManager.h"
#include "Hyperspace/Session.h"

#include "MasterClient.h"
#include "Table.h"
#include "TableScanner.h"
#include "TableMutator.h"

namespace Hypertable {

  class Comm;
  class HqlInterpreter;

  class Client : public ReferenceCount {
  public:

    /**
     * Constructs the object using the specified config file
     *
     * @param install_dir path to Hypertable installation directory
     * @param config_file name of configuration file
     * @param default_timeout_ms default method call timeout in milliseconds
     */
    Client(const String &install_dir, const String &config_file,
           uint32_t default_timeout_ms=0);

    /**
     * Constructs the object using the default config file
     *
     * @param install_dir path to Hypertable installation directory
     * @param default_timeout_ms default method call timeout in milliseconds
     */
    Client(const String &install_dir = String(), uint32_t default_timeout_ms=0);
    ~Client() {}

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
     * @return pointer to newly created Table object
     */
    Table *open_table(const String &name, bool force = false);

    /**
     * Refreshes the cached table entry
     *
     * @param name name of the table
     */
    void refresh_table(const String &name);

    /**
     * Returns the table identifier for a table
     *
     * @param name name of table
     * @return numeric identifier for the table
     */
    uint32_t get_table_id(const String &name);

    /**
     * Returns the schema for a table
     *
     * @param name table name
     * @param with_ids include generation and column family ID attributes
     * @return XML schema of table
     */
    String get_schema(const String &name, bool with_ids=false);

    /**
     * Returns a list of existing table names
     *
     * @param tables reference to vector of table names
     */
    void get_tables(std::vector<String> &tables);

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
     * Return a smart pointer to the Hyperspace session object.
     *
     * @return pointer to the established Hyperspace session
     */
    Hyperspace::SessionPtr& get_hyperspace_session();

    /**
     * Close server logs
     */
    void close();

    /**
     * Shuts down servers
     */
    void shutdown();

    /**
     * Creates an HQL interpreter
     *
     * @return a newly created interpreter object
     */
    HqlInterpreter *create_hql_interpreter();

  private:
    typedef hash_map<String, TablePtr> TableCache;

    void initialize();

    PropertiesPtr           m_props;
    Comm                   *m_comm;
    ConnectionManagerPtr    m_conn_manager;
    ApplicationQueuePtr     m_app_queue;
    Hyperspace::SessionPtr  m_hyperspace;
    MasterClientPtr         m_master_client;
    RangeLocatorPtr         m_range_locator;
    uint32_t                m_timeout_ms;
    String                  m_install_dir;
    TableCache              m_table_cache;
    Mutex                   m_mutex;
    bool                    m_hyperspace_reconnect;
    bool                    m_refresh_schema;
  };

  typedef intrusive_ptr<Client> ClientPtr;

} // namespace Hypertable

#endif // HYPERTABLE_CLIENT_H
