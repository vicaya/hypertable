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

#ifndef HYPERTABLE_SCHEMA_H
#define HYPERTABLE_SCHEMA_H

#include <list>

#include <expat.h>

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/HashMap.h"
#include "Common/Properties.h"


namespace Hypertable {

  enum BloomFilterMode {
    BLOOM_FILTER_DISABLED,
    BLOOM_FILTER_ROWS,
    BLOOM_FILTER_ROWS_COLS
  };

  class Schema : public ReferenceCount {
  public:
    struct ColumnFamily {
      ColumnFamily() : name(), ag(), id(0), max_versions(0), ttl(0),
                       generation(0), deleted(false), renamed(false), new_name() { return; }
      String   name;
      String   ag;
      uint32_t id;
      uint32_t max_versions;
      time_t   ttl;
      uint32_t generation;
      bool deleted;
      bool renamed;
      String new_name;
    };

    typedef std::vector<ColumnFamily *> ColumnFamilies;

    struct AccessGroup {
      AccessGroup() : name(), in_memory(false), replication(-1), blocksize(0),
          bloom_filter(), columns() { }

      String   name;
      bool     in_memory;
      int16_t  replication;
      uint32_t blocksize;
      String compressor;
      String bloom_filter;
      ColumnFamilies columns;
    };

    typedef std::vector<AccessGroup *> AccessGroups;

    Schema(bool read_ids=false);
    Schema(const Schema &src_schema);
    ~Schema();

    static Schema *new_instance(const String &buf, int len, bool read_ids=false);

    static void parse_compressor(const String &spec, PropertiesPtr &);
    void validate_compressor(const String &spec);
    static const PropertiesDesc &compressor_spec_desc();

    static void parse_bloom_filter(const String &spec, PropertiesPtr &);
    void validate_bloom_filter(const String &spec);
    static const PropertiesDesc &bloom_filter_spec_desc();

    void open_access_group();
    void close_access_group();
    void open_column_family();
    void close_column_family();
    void set_access_group_parameter(const char *param, const char *value);
    void set_column_family_parameter(const char *param, const char *value);

    void assign_ids();

    void render(String &output, bool with_ids=false);

    void render_hql_create_table(const String &table_name, String &output);

    bool is_valid();

    const char *get_error_string() {
      return (m_error_string.length() == 0) ? 0 : m_error_string.c_str();
    }

    void set_error_string(const String &errstr) {
      if (m_error_string.length() == 0)
        m_error_string = errstr;
    }

    void incr_generation() { m_generation++; }
    void set_generation(const char *generation);
    void set_max_column_family_id(const char *generation);
    uint32_t get_generation() const { return m_generation; }

    size_t get_max_column_family_id() { return m_max_column_family_id; }
    void incr_max_column_family_id() {++m_max_column_family_id; }

    AccessGroups &
    get_access_groups() { return m_access_groups; }

    AccessGroup *
    get_access_group(const String &name) { return m_access_group_map[name]; }
    ColumnFamilies &
    get_column_families() { return m_column_families; }

    ColumnFamily *
    get_column_family(const String &name, bool get_deleted=false) {
      ColumnFamilyMap::iterator iter = m_column_family_map.find(name);

      if (iter != m_column_family_map.end()) {
        if (get_deleted || iter->second->deleted == false)
          return (iter->second);
      }
      return (ColumnFamily*) 0;
    }

    ColumnFamily *
    get_column_family(uint32_t id, bool get_deleted=false) {
      ColumnFamilyIdMap::iterator iter = m_column_family_id_map.find(id);
      if (iter != m_column_family_id_map.end()) {
        if (get_deleted || iter->second->deleted==false)
          return (iter->second);
      }
      return (ColumnFamily*) 0;
    }

    bool column_family_exists(uint32_t id, bool get_deleted = false) const;
    bool access_group_exists(const String &ag_name) const;

    bool add_access_group(AccessGroup *ag);

    bool add_column_family(ColumnFamily *cf);
    bool drop_column_family(const String& name);
    bool rename_column_family(const String& old_name, const String& new_name);

    void set_compressor(const String &compressor) { m_compressor = compressor; }
    const String &get_compressor() { return m_compressor; }

    typedef hash_map<String, ColumnFamily *> ColumnFamilyMap;
    typedef hash_map<String, AccessGroup *> AccessGroupMap;

    static const uint32_t      ms_max_column_id;

  private:
    typedef hash_map<uint32_t, ColumnFamily *> ColumnFamilyIdMap;

    String m_error_string;
    int    m_next_column_id;
    AccessGroupMap m_access_group_map;
    ColumnFamilies m_column_families; // preserve order
    ColumnFamilyMap m_column_family_map;
    ColumnFamilyIdMap m_column_family_id_map;
    uint32_t m_generation;
    AccessGroups   m_access_groups;
    AccessGroup   *m_open_access_group;
    ColumnFamily  *m_open_column_family;
    bool           m_read_ids;
    bool           m_output_ids;
    size_t         m_max_column_family_id;
    String         m_compressor;

    static void
    start_element_handler(void *userdata, const XML_Char *name,
                          const XML_Char **atts);
    static void end_element_handler(void *userdata, const XML_Char *name);
    static void
    character_data_handler(void *userdata, const XML_Char *s, int len);

    static Schema       *ms_schema;
    static String        ms_collected_text;
    static Mutex      ms_mutex;
  };

  typedef intrusive_ptr<Schema> SchemaPtr;

} // namespace Hypertable

#endif // HYPERTABLE_SCHEMA_H
