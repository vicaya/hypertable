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

#include <cctype>
#include <iostream>
#include <string>

#include <boost/algorithm/string.hpp>

#include <expat.h>

extern "C" {
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
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
std::string Schema::ms_collected_text = "";
boost::mutex Schema::ms_mutex;


/**
 */
Schema::Schema(bool readIds) : m_error_string(), m_next_column_id(0), m_access_group_map(), m_column_family_map(), m_generation(1), m_access_groups(), m_open_access_group(0), m_open_column_family(0), m_read_ids(readIds), m_output_ids(false), m_max_column_family_id(0) {
  if (Logger::logger == 0) {
    cerr << "Logger::initialize must be called before using Schema class" << endl;
    exit(1);
  }
}


Schema::~Schema() {
  for (list<AccessGroup *>::const_iterator agiter = m_access_groups.begin(); agiter != m_access_groups.end(); agiter++)
    delete *agiter;
  for (ColumnFamilyMapT::const_iterator iter = m_column_family_map.begin(); iter != m_column_family_map.end(); iter++)
    delete (*iter).second;
}


Schema *Schema::new_instance(const char *buf, int len, bool readIds) {
  boost::mutex::scoped_lock lock(ms_mutex);
  XML_Parser parser = XML_ParserCreate("US-ASCII");

  XML_SetElementHandler(parser, &start_element_handler, &end_element_handler);
  XML_SetCharacterDataHandler(parser, &character_data_handler);

  ms_schema = new Schema(readIds);

  if (XML_Parse(parser, buf, len, 1) == 0) {
    std::string errStr = (std::string)"Schema Parse Error: " + (const char *)XML_ErrorString(XML_GetErrorCode(parser)) + " line " + (int)XML_GetCurrentLineNumber(parser) + ", offset " + (int)XML_GetCurrentByteIndex(parser);
    ms_schema->set_error_string(errStr);
  }

  XML_ParserFree(parser);  

  Schema *schema = ms_schema;
  ms_schema = 0;
  return schema;
}


/**
 */
void Schema::start_element_handler(void *userData,
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
	ms_schema->set_compressor((std::string)atts[i+1]);
      else
	ms_schema->set_error_string((string)"Unrecognized 'Schema' attribute : " + atts[i]);
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
  else if (!strcasecmp(name, "MaxVersions") || !strcasecmp(name, "ttl") || !strcasecmp(name, "Name"))
    ms_collected_text = "";
  else
    ms_schema->set_error_string((string)"Unrecognized element - '" + name + "'");

  return;
}


void Schema::end_element_handler(void *userData, const XML_Char *name) {
  if (!strcasecmp(name, "AccessGroup"))
    ms_schema->close_access_group();
  else if (!strcasecmp(name, "ColumnFamily"))
    ms_schema->close_column_family();
  else if (!strcasecmp(name, "MaxVersions") || !strcasecmp(name, "ttl") || !strcasecmp(name, "Name")) {
    boost::trim(ms_collected_text);
    ms_schema->set_column_family_parameter(name, ms_collected_text.c_str());
  }
}



void Schema::character_data_handler(void *userData, const XML_Char *s, int len) {
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
      set_error_string((string)"Name attribute required for all AccessGroup elements");
    else {
      if (m_access_group_map.find(m_open_access_group->name) != m_access_group_map.end())
	set_error_string((string)"Multiply defined locality group  '" + m_open_access_group->name + "'");
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
      set_error_string((string)"No id specifid for ColumnFamily '" + m_open_column_family->name + "'");
    }
    else {
      if (m_column_family_map.find(m_open_column_family->name) != m_column_family_map.end())
	set_error_string((string)"Multiply defined column families '" + m_open_column_family->name + "'");
      else {
	m_column_family_map[m_open_column_family->name] = m_open_column_family;
	if (m_read_ids)
	  m_column_family_id_map[m_open_column_family->id] = m_open_column_family;
	m_open_access_group->columns.push_back(m_open_column_family);
      }
      m_open_column_family = 0;
    }
  }
}



void Schema::set_access_group_parameter(const char *param, const char *value) {
  if (m_open_access_group == 0)
    set_error_string((string)"Unable to set AccessGroup attribute '" + param + "', locality group not open");
  else {
    if (!strcasecmp(param, "name"))
      m_open_access_group->name = value;
    else if (!strcasecmp(param, "inMemory")) {
      if (!strcasecmp(value, "true") || !strcmp(value, "1"))
	m_open_access_group->in_memory = true;
      else if (!strcasecmp(value, "false") || !strcmp(value, "0"))
	m_open_access_group->in_memory = false;
      else
	set_error_string((string)"Invalid value (" + value + ") for AccessGroup attribute '" + param + "'");
    }
    else if (!strcasecmp(param, "blockSize")) {
      long long blocksize = strtoll(value, 0, 10);
      if (blocksize == 0 || blocksize >= 4294967296LL)
	set_error_string((string)"Invalid value (" + value + ") for AccessGroup attribute '" + param + "'");
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
    set_error_string((string)"Unable to set ColumnFamily parameter '" + param + "', column family not open");
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
	set_error_string((string)"Invalid value (" + value + ") for MaxVersions");
    }
    else if (m_read_ids && !strcasecmp(param, "id")) {
      m_open_column_family->id = atoi(value);
      if (m_open_column_family->id == 0)
	set_error_string((string)"Invalid value (" + value + ") for ColumnFamily id attribute");
      if (m_open_column_family->id > m_max_column_family_id)
	m_max_column_family_id = m_open_column_family->id;
    }
    else
      set_error_string((string)"Invalid ColumnFamily parameter '" + param + "'");
  }
}


void Schema::assign_ids() {
  m_max_column_family_id = 0;
  for (list<AccessGroup *>::iterator agIter = m_access_groups.begin(); agIter != m_access_groups.end(); agIter++) {
    for (list<ColumnFamily *>::iterator cfIter = (*agIter)->columns.begin(); cfIter != (*agIter)->columns.end(); cfIter++) {
      (*cfIter)->id = ++m_max_column_family_id;
    }
  }
  m_generation = 1;
  m_output_ids=true;
}


void Schema::render(std::string &output) {
  char buf[64];

  if (!is_valid()) {
    output = m_error_string;
    return;
  }

  output += "<Schema";
  if (m_output_ids)
    output += (std::string)" generation=\"" + (uint32_t)m_generation + "\"";
  if (m_compressor != "")
    output += (std::string)" compressor=\"" + m_compressor + "\"";
  output += ">\n";

  for (list<AccessGroup *>::iterator iter = m_access_groups.begin(); iter != m_access_groups.end(); iter++) {
    output += (string)"  <AccessGroup name=\"" + (*iter)->name + "\"";
    if ((*iter)->in_memory)
      output += " inMemory=\"true\"";
    if ((*iter)->blocksize > 0)
      output += (std::string)" blockSize=\"" + (*iter)->blocksize + "\"";
    if ((*iter)->compressor != "")
      output += (std::string)" compressor=\"" + (*iter)->compressor + "\"";
    output += ">\n";
    for (list<ColumnFamily *>::iterator cfiter = (*iter)->columns.begin(); cfiter != (*iter)->columns.end(); cfiter++) {
      output += (string)"    <ColumnFamily";
      if (m_output_ids) {
	output += " id=\"";
	sprintf(buf, "%d", (*cfiter)->id);
	output += (string)buf + "\"";
      }
      output += ">\n";
      output += (string)"      <Name>" + (*cfiter)->name + "</Name>\n";
      if ((*cfiter)->max_versions != 0)
	output += (string)"      <MaxVersions>" + (*cfiter)->max_versions + "</MaxVersions>\n";
      if ((*cfiter)->ttl != 0)
	output += (string)"      <ttl>" + (uint32_t)(*cfiter)->ttl + "</ttl>\n";
      output += (string)"    </ColumnFamily>\n";
    }
    output += (string)"  </AccessGroup>\n";
  }

  output += "</Schema>\n";
  return;
}

void Schema::render_hql_create_table(std::string table_name, std::string &output) {
  bool needs_quotes = false;
  const char *ptr;

  output += "\n";
  output += "CREATE TABLE ";
  if (m_compressor != "")
    output += (std::string)"COMPRESSOR=\"" + m_compressor + "\" ";
  output += table_name + " (\n";

  for (ColumnFamilyMapT::const_iterator cf_iter = m_column_family_map.begin(); cf_iter != m_column_family_map.end(); cf_iter++) {

    // check to see if column family name needs quotes around it
    needs_quotes = false;
    ptr = (*cf_iter).first.c_str();
    if (!isalpha(*ptr))
      needs_quotes = true;
    else {
      for (; *ptr; ptr++) {
	if (!isalnum(*ptr)) {
	  needs_quotes = true;
	  break;
	}
      }
    }

    if (needs_quotes)
      output += (std::string)"  '" + (*cf_iter).first + "'";
    else
      output += (std::string)"  " + (*cf_iter).first;
    if ((*cf_iter).second->max_versions != 0)
      output += (std::string)" MAX_VERSIONS=" + (*cf_iter).second->max_versions;
    if ((*cf_iter).second->ttl != 0)
      output += (std::string)" TTL=" + (uint32_t)(*cf_iter).second->ttl;
    output += (std::string)",\n";
  }

  size_t i=1;
  for (list<AccessGroup *>::iterator ag_iter = m_access_groups.begin(); ag_iter != m_access_groups.end(); ag_iter++, i++) {
    output += (std::string)"  ACCESS GROUP ";

    needs_quotes = false;
    ptr = (*ag_iter)->name.c_str();
    if (!isalpha(*ptr))
      needs_quotes = true;
    else {
      for (++ptr; *ptr; ptr++) {
	if (!isalnum(*ptr)) {
	  needs_quotes = true;
	  break;
	}
      }
    }
    if (needs_quotes)
      output += (std::string)"'" + (*ag_iter)->name + "'";
    else
      output += (*ag_iter)->name;

    if ((*ag_iter)->in_memory)
      output += (std::string)" IN_MEMORY";
    if ((*ag_iter)->blocksize != 0)
      output += (std::string)" BLOCKSIZE=" + (*ag_iter)->blocksize;
    if ((*ag_iter)->compressor != "")
      output += (std::string)" COMPRESSOR=\"" + (*ag_iter)->compressor + "\"";
    if (!(*ag_iter)->columns.empty()) {
      bool display_comma = false;
      output += (std::string)" (";
      for (list<ColumnFamily *>::iterator cfl_iter = (*ag_iter)->columns.begin(); cfl_iter != (*ag_iter)->columns.end(); cfl_iter++) {

	// check to see if column family name needs quotes around it
	needs_quotes = false;
	ptr = (*cfl_iter)->name.c_str();
	if (!isalpha(*ptr))
	  needs_quotes = true;
	else {
	  for (++ptr; *ptr; ptr++) {
	    if (!isalnum(*ptr)) {
	      needs_quotes = true;
	      break;
	    }
	  }
	}

	if (display_comma)
	  output += (std::string)",";
	else
	  display_comma = true;

	if (needs_quotes)
	  output += (std::string)" '" + (*cfl_iter)->name + "'";
	else
	  output += (std::string)" " + (*cfl_iter)->name;
      }
      output += (std::string)" )";
    }
    if (i == m_access_groups.size())
      output += (std::string)"\n";
    else
      output += (std::string)",\n";
  }

  output += ")\n";
  output += "\n";
  
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
  int genNum = atoi(generation);
  if (genNum <= 0)
    set_error_string((string)"Invalid Table 'generation' value (" + generation + ")");
  m_generation = genNum;
}


void Schema::add_access_group(AccessGroup *ag) {
  AccessGroupMapT::const_iterator iter = m_access_group_map.find(ag->name);

  if (iter != m_access_group_map.end()) {
    m_error_string = std::string("Access group '") + ag->name + "' multiply defined";
    delete ag;
    return;
  }

  m_access_group_map[ag->name] = ag;
  m_access_groups.push_back(ag);
}

void Schema::add_column_family(ColumnFamily *cf) {
  ColumnFamilyMapT::const_iterator cf_iter = m_column_family_map.find(cf->name);

  if (cf_iter != m_column_family_map.end()) {
    m_error_string = std::string("Column family '") + cf->name + "' multiply defined";
    delete cf;
    return;
  }

  AccessGroupMapT::const_iterator ag_iter = m_access_group_map.find(cf->ag);

  if (ag_iter == m_access_group_map.end()) {
    m_error_string = std::string("Invalid access group '") + cf->ag + "' for column family '" + cf->name + "'";
    delete cf;
    return;
  }

  m_column_family_map[cf->name] = cf;
  (*ag_iter).second->columns.push_back(cf);

}
