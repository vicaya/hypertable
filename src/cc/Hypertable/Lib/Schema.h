/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_SCHEMA_H
#define HYPERTABLE_SCHEMA_H

#include <list>

#include <string>

#include <boost/intrusive_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include <expat.h>

#include "Common/Properties.h"
#include "Common/ReferenceCount.h"

using namespace Hypertable;
using namespace std;

namespace Hypertable {

  class Schema : public ReferenceCount {
  public:

    class ColumnFamily {
    public:
      ColumnFamily() : name(), ag(), id(0), max_versions(0), ttl(0) { return; }
      string   name;
      string   ag;
      uint32_t id;
      uint32_t max_versions;
      time_t   ttl;
    };

    class AccessGroup {
    public:
      AccessGroup() : name(), in_memory(false), blocksize(0), columns() { return; }
      string name;
      bool     in_memory;
      uint32_t blocksize;
      std::string compressor;
      list<ColumnFamily *> columns;
    };

    Schema(bool readIds=false);
    ~Schema();

    static Schema *new_instance(const char *buf, int len, bool readIds=false);
    friend Schema *new_instance(const char *buf, int len, bool readIds=false);

    void open_access_group();
    void close_access_group();
    void open_column_family();
    void close_column_family();
    void set_access_group_parameter(const char *param, const char *value);
    void set_column_family_parameter(const char *param, const char *value);

    void assign_ids();

    void render(std::string &output);

    void render_hql_create_table(std::string table_name, std::string &output);

    bool is_valid();

    const char *get_error_string() { 
      return (m_error_string.length() == 0) ? 0 : m_error_string.c_str();
    }

    void set_error_string(std::string errStr) {
      if (m_error_string.length() == 0)
	m_error_string = errStr;
    }

    void set_generation(const char *generation);
    int32_t get_generation() { return m_generation; }

    size_t get_max_column_family_id() { return m_max_column_family_id; }

    list<AccessGroup *> *get_access_group_list() { return &m_access_groups; }

    ColumnFamily *get_column_family(string name) { return m_column_family_map[name]; }

    ColumnFamily *get_column_family(uint32_t id) { return m_column_family_id_map[id]; }

    void add_access_group(AccessGroup *ag);

    void add_column_family(ColumnFamily *cf);

    void set_compressor(std::string compressor) { m_compressor = compressor; }
    std::string &get_compressor() { return m_compressor; }

    typedef __gnu_cxx::hash_map<string, ColumnFamily *> ColumnFamilyMapT;
    typedef __gnu_cxx::hash_map<string, AccessGroup *> AccessGroupMapT;

  private:

    typedef __gnu_cxx::hash_map<uint32_t, ColumnFamily *> ColumnFamilyIdMapT;

    std::string m_error_string;
    int    m_next_column_id;
    AccessGroupMapT m_access_group_map;
    ColumnFamilyMapT m_column_family_map;
    ColumnFamilyIdMapT m_column_family_id_map;
    int32_t m_generation;
    list<AccessGroup *> m_access_groups;
    AccessGroup *m_open_access_group;
    ColumnFamily  *m_open_column_family;
    bool           m_read_ids;
    bool           m_output_ids;
    size_t         m_max_column_family_id;
    std::string    m_compressor;

    static void start_element_handler(void *userData, const XML_Char *name, const XML_Char **atts);
    static void end_element_handler(void *userData, const XML_Char *name);
    static void character_data_handler(void *userData, const XML_Char *s, int len);

    static Schema        *ms_schema;
    static std::string    ms_collected_text;
    static boost::mutex  ms_mutex;
  };

  typedef boost::intrusive_ptr<Schema> SchemaPtr;

}

#endif // HYPERTABLE_SCHEMA_H
