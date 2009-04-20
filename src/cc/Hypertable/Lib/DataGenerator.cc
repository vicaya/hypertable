/**
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

extern "C" {
#include <limits.h>
#include <strings.h>
#include <stdlib.h>
}

#include <boost/algorithm/string.hpp>

#include "DataGenerator.h"

using namespace Hypertable;
using namespace Hypertable::Config;


DataGeneratorIterator::DataGeneratorIterator(DataGenerator *generator)
  : m_generator(generator), m_amount(0) {
  RowComponent *row_comp;
  Column *column;

  for (size_t i=0; i<generator->m_row_component_specs.size(); i++) {
    if (generator->m_row_component_specs[i].type == INTEGER)
      row_comp = new RowComponentInteger( generator->m_row_component_specs[i] );
    else if (generator->m_row_component_specs[i].type == TIMESTAMP)
      row_comp = new RowComponentTimestamp( generator->m_row_component_specs[i] );
    else
      HT_ASSERT(!"Unrecognized row component type");
    m_row_components.push_back(row_comp);
  }

  for (size_t i=0; i<generator->m_column_specs.size(); i++) {
    column = new ColumnString( generator->m_column_specs[i] );
    m_columns.push_back(column);
  }

  m_next_column = m_columns.size() - 1;

  next();
}


void DataGeneratorIterator::next() {
  size_t compi = m_row_components.size();

  HT_ASSERT(compi > 0);

  m_next_column = (m_next_column + 1) % m_columns.size();

  if (m_next_column == 0) {
    do {
      compi--;
      if (m_row_components[compi]->next())
        break;
    } while (compi > 0);

    m_row = "";

    for (size_t i=0; i<m_row_components.size(); i++)
      m_row_components[i]->render(m_row);
  }

  m_cell.row_key = m_row.c_str();

  m_columns[m_next_column]->next();

  m_cell.column_family = m_columns[m_next_column]->column_family.c_str();
  m_cell.column_qualifier = m_columns[m_next_column]->qualifier().c_str();
  m_cell.value = (const uint8_t *)m_columns[m_next_column]->value().c_str();
  m_cell.value_len = m_columns[m_next_column]->value().length();

  m_amount += m_row.length() + strlen(m_cell.column_qualifier) + m_cell.value_len;
}

DataGeneratorIterator& DataGeneratorIterator::operator++() {
  next();
  return *this;
}

DataGeneratorIterator& DataGeneratorIterator::operator++(int n) {
  next();
  for (int i=1; i<n; i++)
    next();
  return *this;
}



DataGenerator::DataGenerator() {
  int order;
  String str;
  std::map<String, int> column_map;

  m_limit = get_i64("DataGenerator.MaxBytes", std::numeric_limits<int64_t>::max());
  m_seed = get_i32("DataGenerator.Seed", 1);

  str = get_str("rowkey.order", "ascending");
  if (!strcasecmp(str.c_str(), "ascending"))
    order = ASCENDING;
  else if (!strcasecmp(str.c_str(), "random"))
    order = RANDOM;
  else
    HT_THROW(Error::SYNTAX_ERROR, format("Unsupported rowkey order - %s", str.c_str()));

  std::vector<String> names;
  String name;
  Config::properties->get_names(names);
  long index = 0;
  char *ptr, *tptr;

  for (size_t i=0; i<names.size(); i++) {
    if (!strncmp(names[i].c_str(), "rowkey.component.", 17)) {
      index = strtol(names[i].c_str()+17, &ptr, 0);
      if (index < 0 || index > 100)
        HT_THROW(Error::SYNTAX_ERROR, format("Bad format for key %s", names[i].c_str()));
      if (m_row_component_specs.size() <= (size_t)index)
        m_row_component_specs.resize(index+1);
      if (!strcmp(ptr, ".type")) {
        str = get_str(names[i]);
        if (!strcasecmp(str.c_str(), "integer"))
          m_row_component_specs[index].type = INTEGER;
        else if (!strcasecmp(str.c_str(), "timestamp"))
          m_row_component_specs[index].type = TIMESTAMP;
        else
          HT_THROW(Error::SYNTAX_ERROR, format("Invalid rowkey component type - %s", str.c_str()));
      }
      else if (!strcmp(ptr, ".format")) {
        m_row_component_specs[index].format = get_str(names[i]);
        boost::trim_if(m_row_component_specs[index].format, boost::is_any_of("'\""));
      }
      else if (!strcmp(ptr, ".min")) {
        m_row_component_specs[index].min = get_str(names[i]);
      }
      else if (!strcmp(ptr, ".max")) {
        m_row_component_specs[index].max = get_str(names[i]);
      }
      else
        HT_THROW(Error::SYNTAX_ERROR, format("Invalid key - %s", names[i].c_str()));
    }
    else if (strstr(names[i].c_str(), ".qualifier.") || strstr(names[i].c_str(), ".value.")) {
      int columni;
      name = String("") + names[i];
      tptr = strchr((char *)name.c_str(), '.');
      *tptr++ = 0;

      std::map<String, int>::iterator iter = column_map.find((String)name.c_str());
      if (iter == column_map.end()) {
        columni = column_map.size();
        column_map[(String)name.c_str()] = columni;
        m_column_specs.push_back(ColumnSpec());
        m_column_specs[columni].column_family = name;
      }
      else
        columni = (*iter).second;

      str = get_str(names[i]);

      if (!strcmp(tptr, "qualifier.type")) {
        if (!strcasecmp(str.c_str(), "STRING"))
          m_column_specs[columni].qualifier.type = STRING;
        else
          HT_THROW(Error::SYNTAX_ERROR, format("Unsupported type (%s) for '%s'",
                                               str.c_str(), names[i].c_str()));
      }
      else if (!strcmp(tptr, "qualifier.size")) {
        m_column_specs[columni].qualifier.size = atoi(str.c_str());
      }
      else if (!strcmp(tptr, "qualifier.charset")) {
        m_column_specs[columni].qualifier.charset = str;
        boost::trim_if(m_column_specs[columni].qualifier.charset, boost::is_any_of("'\""));
      }
      else if (!strcmp(tptr, "value.size")) {
        m_column_specs[columni].size = atoi(str.c_str());
      }
      else if (!strcmp(tptr, "value.source")) {
        m_column_specs[columni].source = str;
      }
    }
  }

  for (size_t i=0; i<m_column_specs.size(); i++) {
    if (m_column_specs[i].qualifier.type == -1)
      m_column_specs[i].qualifier.type = STRING;
    if (m_column_specs[i].qualifier.size == -1)
      HT_THROW(Error::SYNTAX_ERROR,
               format("No qualifier size specified for column '%s'",
                      m_column_specs[i].column_family.c_str()));
    if (m_column_specs[i].qualifier.charset == "")
      HT_THROW(Error::SYNTAX_ERROR,
               format("No qualifier charset specified for column '%s'",
                      m_column_specs[i].column_family.c_str()));
    if (m_column_specs[i].size == -1)
      HT_THROW(Error::SYNTAX_ERROR,
               format("No value size specified for column '%s'",
                      m_column_specs[i].column_family.c_str()));
    if (m_column_specs[i].source == "")
      HT_THROW(Error::SYNTAX_ERROR,
               format("No value source specified for column '%s'",
                      m_column_specs[i].column_family.c_str()));
  }

  for (size_t i=0; i<m_row_component_specs.size(); i++) {
    if (m_row_component_specs[i].order == -1)
      m_row_component_specs[i].order = order;
    if (m_row_component_specs[i].type == -1)
      HT_FATALF("Missing type for component %lu", i);
    else if (m_row_component_specs[i].type == INTEGER &&
             m_row_component_specs[i].format != "") {
      if (!strstr(m_row_component_specs[i].format.c_str(), "lld"))
        HT_FATALF("Format sequence (%s) must contain 'lld'",
                  m_row_component_specs[i].format.c_str());
    }
  }

}

