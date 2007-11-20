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

  if (!strcmp(name, "Schema")) {
    for (i=0; atts[i] != 0; i+=2) {
      if (atts[i+1] == 0)
	return;
      if (ms_schema->m_read_ids && !strcmp(atts[i], "generation"))
	ms_schema->set_generation(atts[i+1]);
      else
	ms_schema->set_error_string((string)"Unrecognized 'Schema' attribute : " + atts[i]);
    }
  }
  else if (!strcmp(name, "AccessGroup")) {
    ms_schema->open_access_group();    
    for (i=0; atts[i] != 0; i+=2) {
      if (atts[i+1] == 0)
	return;
      ms_schema->set_access_group_parameter(atts[i], atts[i+1]);
    }
  }
  else if (!strcmp(name, "ColumnFamily")) {
    ms_schema->open_column_family();
    for (i=0; atts[i] != 0; i+=2) {
      if (atts[i+1] == 0)
	return;
      ms_schema->set_column_family_parameter(atts[i], atts[i+1]);
    }
  }
  else if (!strcmp(name, "CellLimit") || !strcmp(name, "ExpireDays") || !strcmp(name, "Name"))
    ms_collected_text = "";
  else
    ms_schema->set_error_string((string)"Unrecognized element - '" + name + "'");

  return;
}


void Schema::end_element_handler(void *userData, const XML_Char *name) {
  if (!strcmp(name, "AccessGroup"))
    ms_schema->close_access_group();
  else if (!strcmp(name, "ColumnFamily"))
    ms_schema->close_column_family();
  else if (!strcmp(name, "CellLimit") || !strcmp(name, "ExpireDays") || !strcmp(name, "Name")) {
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
      m_open_column_family->cellLimit = 0;
      m_open_column_family->expireTime = 0;
      m_open_column_family->lg = m_open_access_group->name;
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
	m_open_access_group->inMemory = true;
      else if (!strcasecmp(value, "false") || !strcmp(value, "0"))
	m_open_access_group->inMemory = false;
      else
	set_error_string((string)"Invalid value (" + value + ") for AccessGroup attribute '" + param + "'");
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
    else if (!strcasecmp(param, "ExpireDays")) {
      float fval = strtof(value, 0);
      if (fval == 0.0) {
	set_error_string((string)"Invalid value (" + value + ") for ExpireDays");
      }
      else {
	fval *= 86400.0;
	m_open_column_family->expireTime = (time_t)fval;
      }
    }
    else if (!strcasecmp(param, "CellLimit")) {
      m_open_column_family->cellLimit = atoi(value);
      if (m_open_column_family->cellLimit == 0)
	set_error_string((string)"Invalid value (" + value + ") for CellLimit");
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
  for (list<AccessGroup *>::iterator lgIter = m_access_groups.begin(); lgIter != m_access_groups.end(); lgIter++) {
    for (list<ColumnFamily *>::iterator cfIter = (*lgIter)->columns.begin(); cfIter != (*lgIter)->columns.end(); cfIter++) {
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
  if (m_output_ids) {
    output += " generation=\"";
    sprintf(buf, "%d", m_generation);
    output += (string)buf + "\"";
  }
  output += ">\n";

  for (list<AccessGroup *>::iterator iter = m_access_groups.begin(); iter != m_access_groups.end(); iter++) {
    output += (string)"  <AccessGroup name=\"" + (*iter)->name + "\"";
    if ((*iter)->inMemory)
      output += " inMemory=\"true\"";
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
      if ((*cfiter)->cellLimit != 0) {
	sprintf(buf, "%d", (*cfiter)->cellLimit);
	output += (string)"        <CellLimit>" + buf + "</CellLimit>\n";
      }
      if ((*cfiter)->expireTime != 0) {
	float fdays = (float)(*cfiter)->expireTime / 86400.0;
	sprintf(buf, "%.2f", fdays);
	output += (string)"        <ExpireDays>" + buf + "</ExpireDays>\n";
      }
      output += (string)"    </ColumnFamily>\n";
    }
    output += (string)"  </AccessGroup>\n";
  }

  output += "</Schema>\n";
  return;
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
}

void Schema::add_column_family(ColumnFamily *cf) {
  ColumnFamilyMapT::const_iterator cf_iter = m_column_family_map.find(cf->name);
  
  if (cf_iter != m_column_family_map.end()) {
    m_error_string = std::string("Column family '") + cf->name + "' multiply defined";
    delete cf;
    return;
  }

  AccessGroupMapT::const_iterator ag_iter = m_access_group_map.find(cf->lg);

  if (ag_iter == m_access_group_map.end()) {
    m_error_string = std::string("Invalid access group '") + cf->lg + "' for column family '" + cf->name + "'";
    delete cf;
    return;
  }

  m_column_family_map[cf->name] = cf;
  (*ag_iter).second->columns.push_back(cf);

}
