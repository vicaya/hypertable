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

#ifndef HYPERTABLE_CLIENT_H
#define HYPERTABLE_CLIENT_H

#include <string>

#include "Common/Properties.h"
#include "Common/ReferenceCount.h"
#include "Common/String.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/ConnectionManager.h"
#include "Hyperspace/Session.h"

#include "MasterClient.h"
#include "Table.h"


namespace Hypertable {

  class Comm;
  class HqlCommandInterpreter;

  class Client : public ReferenceCount {

  public:

    /**
     * Constructs the object using the specified config file
     *
     * @param argv0 the zero'th argument in the vector passed into main
     * @param config_file name of configuration file
     */
    Client(const char *argv0, const String &config_file);

    /**
     * Constructs the object using the default config file
     *
     * @param argv0 the zero'th argument in the vector passed into main
     */
    Client(const char *argv0);

    /**
     * Creates a table
     *
     * @param name name of the table
     * @param schema schema definition for the table
     */
    void create_table(const String &name, const String &schema);

    /**
     * Opens a table
     *
     * @param name name of the table
     * @return pointer to newly created Table object
     */
    Table *open_table(const String &name);

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
     * Creates an HQL command interpreter
     *
     * @return a newly created interpreter object
     */
    HqlCommandInterpreter *create_hql_interpreter();

  private:

    void initialize(const String &config_file);

    PropertiesPtr           m_props_ptr;
    Comm                   *m_comm;
    ConnectionManagerPtr    m_conn_manager_ptr;
    ApplicationQueuePtr     m_app_queue_ptr;
    Hyperspace::SessionPtr  m_hyperspace_ptr;
    MasterClientPtr         m_master_client_ptr;
  };
  typedef boost::intrusive_ptr<Client> ClientPtr;


}

#endif // HYPERTABLE_CLIENT_H
