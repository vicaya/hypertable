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

#include "Schema.h"

using namespace Hypertable;
using namespace Hypertable;
using namespace std;

Schema *Schema::ms_schema = 0;
String Schema::ms_collected_text = "";
Mutex        Schema::ms_mutex;

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

} // local namespace


/**
 */
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
           || !strcasecmp(name, "Name"))
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
           || !strcasecmp(name, "Name")) {
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
