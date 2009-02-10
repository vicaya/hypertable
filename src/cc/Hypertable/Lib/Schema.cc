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
#include <cctype>
#include <iostream>
#include <string>

#include <boost/algorithm/string.hpp>

#include <expat.h>

extern "C" {
#include <ctype.h>
#include <errno.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
}

#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"
#include "Common/System.h"
#include "Common/Config.h"

#include "Schema.h"

using namespace Hypertable;
using namespace Property;
using namespace std;

Schema *Schema::ms_schema = 0;
String Schema::ms_collected_text = "";
Mutex Schema::ms_mutex;

namespace {

bool hql_needs_quotes(const char *name) {
  if (!isalpha(*name))
    return true;
  else {
    for (; *name; name++) {
      if (!isalnum(*name)) {
        return true;
      }
    }
  }
  return false;
}

Mutex desc_mutex;
bool desc_inited = false;

PropertiesDesc
  compressor_desc("  bmz|lzo|quicklz|zlib|none [compressor_options]\n\n"
      "compressor_options"),
  bloom_filter_desc("  rows|rows+cols|none [bloom_filter_options]\n\n"
      "  Default bloom filter is defined by the config property:\n"
      "  Hypertable.RangeServer.CellStore.DefaultBloomFilter.\n\n"
      "bloom_filter_options");

PropertiesDesc compressor_hidden_desc, bloom_filter_hidden_desc;
PositionalDesc compressor_pos_desc, bloom_filter_pos_desc;

void init_schema_options_desc() {
  ScopedLock lock(desc_mutex);

  if (desc_inited)
    return;

  compressor_desc.add_options()
    ("best,9", "Highest setting (probably slower) for zlib")
    ("normal", "Normal setting for zlib")
    ("fp-len", i16()->default_value(19), "Minimum fingerprint length for bmz")
    ("offset", i16()->default_value(0), "Starting fingerprint offset for bmz")
    ;
  compressor_hidden_desc.add_options()
    ("compressor-type", str(), "Compressor type (bmz|lzo|quicklz|zlib|none)")
    ;
  compressor_pos_desc.add("compressor-type", 1);

  bloom_filter_desc.add_options()
    ("false-positive", f64()->default_value(0.01), "Expected false positive "
        "probability for the Bloom filter")
    ("max-approx-items", i32()->default_value(1000), "Number of cell store "
        "items used to guess the number of actual Bloom filter entries")
    ;
  bloom_filter_hidden_desc.add_options()
    ("bloom-filter-mode", str(), "Bloom filter mode (rows|rows+cols|none)")
    ;
  bloom_filter_pos_desc.add("bloom-filter-mode", 1);
  desc_inited = true;
}

} // local namespace


Schema::Schema(bool read_ids)
  : m_error_string(), m_next_column_id(0), m_access_group_map(),
    m_column_family_map(), m_generation(1), m_access_groups(),
    m_open_access_group(0), m_open_column_family(0), m_read_ids(read_ids),
    m_output_ids(false), m_max_column_family_id(0) {
}


Schema::~Schema() {
  foreach(AccessGroup *ag, m_access_groups)
    delete ag;
  foreach(ColumnFamily *cf, m_column_families)
    delete cf;
}


Schema *Schema::new_instance(const char *buf, int len, bool read_ids) {
  ScopedLock lock(ms_mutex);
  XML_Parser parser = XML_ParserCreate("US-ASCII");

  XML_SetElementHandler(parser, &start_element_handler, &end_element_handler);
  XML_SetCharacterDataHandler(parser, &character_data_handler);

  ms_schema = new Schema(read_ids);

  if (XML_Parse(parser, buf, len, 1) == 0) {
    String errstr = (String)"Schema Parse Error: "
        + (const char *)XML_ErrorString(XML_GetErrorCode(parser))
        + " line " + (int)XML_GetCurrentLineNumber(parser) + ", offset "
        + (int)XML_GetCurrentByteIndex(parser);
    ms_schema->set_error_string(errstr);
  }

  XML_ParserFree(parser);

  Schema *schema = ms_schema;
  ms_schema = 0;
  return schema;
}


void Schema::parse_compressor(const String &compressor, PropertiesPtr &props) {
  init_schema_options_desc();

  vector<String> args;
  boost::split(args, compressor, boost::is_any_of(" \t"));
  HT_TRY("parsing compressor spec",
    props->parse_args(args, compressor_desc, &compressor_hidden_desc,
                      &compressor_pos_desc));
}


const PropertiesDesc &Schema::compressor_spec_desc() {
  init_schema_options_desc();
  return compressor_desc;
}


void
Schema::parse_bloom_filter(const String &bloom_filter, PropertiesPtr &props) {
  init_schema_options_desc();

  vector<String> args;

  boost::split(args, bloom_filter, boost::is_any_of(" \t"));
  HT_TRY("parsing bloom filter spec",
    props->parse_args(args, bloom_filter_desc, &bloom_filter_hidden_desc,
                      &bloom_filter_pos_desc));

  String mode = props->get_str("bloom-filter-mode");

  if (mode == "none" || mode == "disabled")
    props->set("bloom-filter-mode", BLOOM_FILTER_DISABLED);
  else if (mode == "rows" || mode == "row")
    props->set("bloom-filter-mode", BLOOM_FILTER_ROWS);
  else if (mode == "rows+cols" || mode == "row+col"
           || mode == "rows-cols" || mode == "row-col"
           || mode == "rows_cols" || mode == "row_col")
    props->set("bloom-filter-mode", BLOOM_FILTER_ROWS_COLS);
  else HT_THROWF(Error::BAD_SCHEMA, "unknown bloom filter mode: '%s'",
                 mode.c_str());
}


const PropertiesDesc &Schema::bloom_filter_spec_desc() {
  init_schema_options_desc();
  return bloom_filter_desc;
}


void Schema::validate_compressor(const String &compressor) {
  if (compressor.empty())
    return;

  try {
    PropertiesPtr props = new Properties();
    parse_compressor(compressor, props);
  }
  catch (Exception &e) {
    ostringstream oss;
    oss << e;
    set_error_string(oss.str());
  }
}


void Schema::validate_bloom_filter(const String &bloom_filter) {
  if (bloom_filter.empty())
    return;

  try {
    PropertiesPtr props = new Properties();
    parse_bloom_filter(bloom_filter, props);
  }
  catch (Exception &e) {
    ostringstream oss;
    oss << e;
    set_error_string(oss.str());
  }
}


/**
 */
void Schema::start_element_handler(void *userdata,
                         const XML_Char *name,
                         const XML_Char **atts) {
  int i;

  if (!strcasecmp(name, "Schema")) {
    for (i=0; atts[i] != 0; i+=2) {
      if (atts[i+1] == 0)
        return;
      if (ms_schema->m_read_ids && !strcasecmp(atts[i], "generation"))
        ms_schema->set_generation(atts[i+1]);
      else if (!strcasecmp(atts[i], "compressor"))
        ms_schema->set_compressor((String)atts[i+1]);
      else
        ms_schema->set_error_string((String)"Unrecognized 'Schema' attribute : "
                                     + atts[i]);
    }
  }
  else if (!strcasecmp(name, "AccessGroup")) {
    ms_schema->open_access_group();
    for (i=0; atts[i] != 0; i+=2) {
      if (atts[i+1] == 0)
        return;
      ms_schema->set_access_group_parameter(atts[i], atts[i+1]);
    }
  }
  else if (!strcasecmp(name, "ColumnFamily")) {
    ms_schema->open_column_family();
    for (i=0; atts[i] != 0; i+=2) {
      if (atts[i+1] == 0)
        return;
      ms_schema->set_column_family_parameter(atts[i], atts[i+1]);
    }
  }
  else if (!strcasecmp(name, "MaxVersions") || !strcasecmp(name, "ttl")
           || !strcasecmp(name, "Name") || !strcasecmp(name, "generation")
           || !strcasecmp(name, "deleted"))
    ms_collected_text = "";
  else
    ms_schema->set_error_string(format("Unrecognized element - '%s'", name));

  return;
}


void Schema::end_element_handler(void *userdata, const XML_Char *name) {
  if (!strcasecmp(name, "AccessGroup"))
    ms_schema->close_access_group();
  else if (!strcasecmp(name, "ColumnFamily"))
    ms_schema->close_column_family();
  else if (!strcasecmp(name, "MaxVersions") || !strcasecmp(name, "ttl")
           || !strcasecmp(name, "Name") || !strcasecmp(name, "generation")
           || !strcasecmp(name, "deleted")) {
    boost::trim(ms_collected_text);
    ms_schema->set_column_family_parameter(name, ms_collected_text.c_str());
  }
}



void
Schema::character_data_handler(void *userdata, const XML_Char *s, int len) {
  ms_collected_text.assign(s, len);
}



void Schema::open_access_group() {
  if (m_open_access_group != 0)
    set_error_string((string)"Nested AccessGroup elements not allowed");
  else {
    m_open_access_group = new AccessGroup();
  }
}



void Schema::close_access_group() {
  if (m_open_access_group == 0)
    set_error_string((string)"Unable to close locality group, not open");
  else {
    if (m_open_access_group->name == "")
      set_error_string("Name attribute required for all AccessGroup elements");
    else {
      if (m_access_group_map.find(m_open_access_group->name)
          != m_access_group_map.end())
        set_error_string((String)"Multiply defined locality group '"
                          + m_open_access_group->name + "'");
      else {
        m_access_group_map[m_open_access_group->name] = m_open_access_group;
        m_access_groups.push_back(m_open_access_group);
      }
    }
    m_open_access_group = 0;
  }
}



void Schema::open_column_family() {
  if (m_open_access_group == 0)
    set_error_string((string)"ColumnFamily defined outside of AccessGroup");
  else {
    if (m_open_column_family != 0)
      set_error_string((string)"Nested ColumnFamily elements not allowed");
    else {
      m_open_column_family = new ColumnFamily();
      m_open_column_family->id = 0;
      m_open_column_family->max_versions = 0;
      m_open_column_family->ttl = 0;
      m_open_column_family->generation = 0;
      m_open_column_family->deleted = false;
      m_open_column_family->ag = m_open_access_group->name;
    }
  }
}



void Schema::close_column_family() {
  if (m_open_access_group == 0)
    set_error_string((string)"ColumnFamily defined outside of AccessGroup");
  else if (m_open_column_family == 0)
    set_error_string((string)"Unable to close column family, not open");
  else {
    if (m_open_column_family->name == "")
      set_error_string((string)"ColumnFamily must have Name child element");
    else if (m_read_ids && m_open_column_family->id == 0) {
      set_error_string((String)"No id specifid for ColumnFamily '"
                        + m_open_column_family->name + "'");
    }
    else {
      if (m_column_family_map.find(m_open_column_family->name)
          != m_column_family_map.end())
        set_error_string((string)"Multiply defined column families '"
                          + m_open_column_family->name + "'");
      else {
        m_column_family_map[m_open_column_family->name] = m_open_column_family;
        if (m_read_ids)
          m_column_family_id_map[m_open_column_family->id] =
              m_open_column_family;
        m_open_access_group->columns.push_back(m_open_column_family);
        m_column_families.push_back(m_open_column_family);
      }
      m_open_column_family = 0;
    }
  }
}



void Schema::set_access_group_parameter(const char *param, const char *value) {
  if (m_open_access_group == 0)
    set_error_string((String)"Unable to set AccessGroup attribute '" + param
                      + "', locality group not open");
  else {
    if (!strcasecmp(param, "name"))
      m_open_access_group->name = value;
    else if (!strcasecmp(param, "inMemory")) {
      if (!strcasecmp(value, "true") || !strcmp(value, "1"))
        m_open_access_group->in_memory = true;
      else if (!strcasecmp(value, "false") || !strcmp(value, "0"))
        m_open_access_group->in_memory = false;
      else
        set_error_string((String)"Invalid value (" + value
                          + ") for AccessGroup attribute '" + param + "'");
    }
    else if (!strcasecmp(param, "blksz")) {
      long long blocksize = strtoll(value, 0, 10);
      if (blocksize == 0 || blocksize >= 4294967296LL)
        set_error_string((String)"Invalid value (" + value
                          + ") for AccessGroup attribute '" + param + "'");
      else
        m_open_access_group->blocksize = (uint32_t)blocksize;
    }
    else if (!strcasecmp(param, "compressor")) {
      m_open_access_group->compressor = value;
      boost::trim(m_open_access_group->compressor);
      validate_compressor(m_open_access_group->compressor);
    }
    else if (!strcasecmp(param, "bloomFilter")) {
      m_open_access_group->bloom_filter = value;
      boost::trim(m_open_access_group->bloom_filter);
      validate_bloom_filter(m_open_access_group->bloom_filter);
    }
    else
      set_error_string((string)"Invalid AccessGroup attribute '" + param + "'");
  }
}



void Schema::set_column_family_parameter(const char *param, const char *value) {
  if (m_open_column_family == 0)
    set_error_string((String)"Unable to set ColumnFamily parameter '" + param
                      + "', column family not open");
  else {
    if (!strcasecmp(param, "Name"))
      m_open_column_family->name = value;
    else if (!strcasecmp(param, "ttl")) {
      long long secs = strtoll(value, 0, 10);
      m_open_column_family->ttl = (time_t)secs;
    }
    else if (!strcasecmp(param, "generation")) {
      m_open_column_family->generation = atoi(value);
    }
    else if (!strcasecmp(param, "deleted")) {
      if(!strcasecmp(value, "true"))
        m_open_column_family->deleted = true;
      else if (!strcasecmp(value, "false"))
        m_open_column_family->deleted = false;
      else
        set_error_string(format("Invalid ColumnFamily value for param '%s'",
            param));
    }
    else if (!strcasecmp(param, "MaxVersions")) {
      m_open_column_family->max_versions = atoi(value);
      if (m_open_column_family->max_versions == 0)
        set_error_string((String)"Invalid value (" + value
                          + ") for MaxVersions");
    }
    else if (m_read_ids && !strcasecmp(param, "id")) {
      m_open_column_family->id = atoi(value);
      if (m_open_column_family->id == 0)
        set_error_string((String)"Invalid value (" + value
                          + ") for ColumnFamily id attribute");
      if (m_open_column_family->id > m_max_column_family_id)
        m_max_column_family_id = m_open_column_family->id;
    }
    else
      set_error_string(format("Invalid ColumnFamily parameter '%s'", param));
  }
}


void Schema::assign_ids() {
  m_max_column_family_id = 0;

  foreach(ColumnFamily *cf, m_column_families)
    cf->id = ++m_max_column_family_id;

  m_generation = 1;
  m_output_ids=true;
}


void Schema::render(String &output) {
  if (!is_valid()) {
    output = m_error_string;
    return;
  }
  output += "<Schema";

  if (m_output_ids)
    output += format(" generation=\"%d\"", m_generation);

  if (m_compressor != "")
    output += format(" compressor=\"%s\"", m_compressor.c_str());

  output += ">\n";

  foreach(const AccessGroup *ag, m_access_groups) {
    output += format("  <AccessGroup name=\"%s\"", ag->name.c_str());

    if (ag->in_memory)
      output += " inMemory=\"true\"";

    if (ag->blocksize > 0)
      output += format(" blksz=\"%u\"", ag->blocksize);

    if (ag->compressor != "")
      output += format(" compressor=\"%s\"", ag->compressor.c_str());

    if (ag->bloom_filter != "")
      output += (String)" bloomFilter=\"" + ag->bloom_filter + "\"";

    output += ">\n";

    foreach(const ColumnFamily *cf, ag->columns) {
      output += "    <ColumnFamily";

      if (m_output_ids)
        output += format(" id=\"%u\"", cf->id);

      output += ">\n";
      output += format("      <Name>%s</Name>\n", cf->name.c_str());

      if (cf->max_versions != 0)
        output += format("      <MaxVersions>%u</MaxVersions>\n",
                         cf->max_versions);

      if (cf->ttl != 0)
        output += format("      <ttl>%d</ttl>\n", (int)cf->ttl);
      
      if (cf->generation != 0 )
        output += format("      <generation>%d</generation>\n", 
                         (int)cf->generation);

      if (cf->deleted == true)
        output += format("      <deleted>true</deleted>\n"); 

      output += "    </ColumnFamily>\n";
    }
    output += "  </AccessGroup>\n";
  }
  output += "</Schema>\n";
}

void Schema::render_hql_create_table(const String &table_name, String &output) {
  output += "\n";
  output += "CREATE TABLE ";

  if (m_compressor != "")
    output += format("COMPRESSOR=\"%s\"", m_compressor.c_str());

  output += table_name + " (\n";

  foreach(const ColumnFamily *cf, m_column_families) {
    // check to see if column family name needs quotes around it
    if (hql_needs_quotes(cf->name.c_str()))
      output += format("  '%s'", cf->name.c_str());
    else
      output += format("  %s", cf->name.c_str());

    if (cf->max_versions != 0)
      output += format(" MAX_VERSIONS=%u", cf->max_versions);

    if (cf->ttl != 0)
      output += format(" TTL=%d", (int)cf->ttl);
    
    if (cf->generation != 0 )
      output += format(" GENERATION=%d", (int)cf->generation);
    
    if (cf->deleted)
      output += " DELETED=TRUE";

    output += ",\n";
  }
  size_t i = 1;

  foreach(const AccessGroup *ag, m_access_groups) {
    output += "  ACCESS GROUP ";

    if (hql_needs_quotes(ag->name.c_str()))
      output += format("'%s'", ag->name.c_str());
    else
      output += ag->name;

    if (ag->in_memory)
      output += " IN_MEMORY";

    if (ag->blocksize != 0)
      output += format(" BLOCKSIZE=%u", ag->blocksize);

    if (ag->compressor != "")
      output += format(" COMPRESSOR=\"%s\"", ag->compressor.c_str());

    if (ag->bloom_filter != "")
      output += format(" BLOOMFILTER=\"%s\"",
          ag->bloom_filter.c_str());

    if (!ag->columns.empty()) {
      bool display_comma = false;
      output += " (";

      foreach(const ColumnFamily *cf, ag->columns) {
        // check to see if column family name needs quotes around it
        if (display_comma)
          output += ", ";
        else
          display_comma = true;

        if (hql_needs_quotes(cf->name.c_str()))
          output += format("'%s'", cf->name.c_str());
        else
          output += cf->name;
      }
      output += ")";
    }
    output += i == m_access_groups.size() ? "\n" : ",\n";
  }
  output += ")\n";
}


/**
 * Protected methods
 */

bool Schema::is_valid() {
  if (get_error_string() != 0)
    return false;
  return true;
}

/**
 *
 */
void Schema::set_generation(const char *generation) {
  int gen_num = atoi(generation);
  if (gen_num <= 0)
    set_error_string((String)"Invalid Table 'generation' value (" + generation
                      + ")");
  m_generation = gen_num;
}


void Schema::add_access_group(AccessGroup *ag) {
  pair<AccessGroupMap::iterator, bool> res =
      m_access_group_map.insert(make_pair(ag->name, ag));

  if (!res.second) {
    m_error_string = String("Access group '") + ag->name + "' multiply defined";
    delete ag;
    return;
  }
  m_access_groups.push_back(ag);
}

void Schema::add_column_family(ColumnFamily *cf) {
  pair<ColumnFamilyMap::iterator, bool> res =
      m_column_family_map.insert(make_pair(cf->name, cf));

  if (!res.second) {
    m_error_string = format("Column family '%s' multiply defined",
                            cf->name.c_str());
    delete cf;
    return;
  }

  AccessGroupMap::const_iterator ag_iter = m_access_group_map.find(cf->ag);

  if (ag_iter == m_access_group_map.end()) {
    m_error_string = String("Invalid access group '") + cf->ag
        + "' for column family '" + cf->name + "'";
    delete cf;
    return;
  }

  m_column_families.push_back(cf);
  ag_iter->second->columns.push_back(cf);
}
