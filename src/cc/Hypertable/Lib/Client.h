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

    /**
     * Creates a table
     *
     * @param name name of the table
     * @param schema schema definition for the table
     */
    void create_table(const String &name, const String &schema);
    
    /**
     * Alters column families within a table 
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
     * @return XML schema of table
     */
    String get_schema(const String &name);

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
  };

  typedef intrusive_ptr<Client> ClientPtr;

} // namespace Hypertable

#endif // HYPERTABLE_CLIENT_H
