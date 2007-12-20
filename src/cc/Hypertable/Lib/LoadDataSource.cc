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

#include <cerrno>
#include <cstring>

#include <boost/algorithm/string.hpp>
#include <boost/shared_array.hpp>

extern "C" {
#include <strings.h>
}

#include "Common/ByteOrder.h"
#include "Common/DynamicBuffer.h"
#include "Common/Error.h"

#include "Key.h"

#include "LoadDataSource.h"

using namespace std;

/**
 *
 */
LoadDataSource::LoadDataSource(std::string fname) : m_fin(fname.c_str()), m_cur_line(0), m_line_buffer(0), m_hyperformat(false), m_leading_timestamps(false) {
  string line;
  char *base, *ptr;

  if (!getline(m_fin, line))
    return;

  base = (char *)line.c_str();
  while ((ptr = strchr(base, '\t')) != 0) {
    *ptr++ = 0;
    m_column_names.push_back(base);
    base = ptr;
  }
  m_column_names.push_back(base);

  if (m_column_names.size() == 3 || m_column_names.size() == 4) {
    size_t i=m_column_names.size()-3;
    if (m_column_names[i] == "rowkey" &&
	m_column_names[i+1] == "columnkey" &&
	m_column_names[i+2] == "value") {
      if (i == 0 || m_column_names[0] == "timestamp") {
	m_hyperformat = true;
	m_leading_timestamps = (i==1);
      }
    }
  }

  if (!m_hyperformat && m_column_names.size() < 2)
    throw Exception(Error::HQL_BAD_LOAD_FILE_FORMAT, "No columns specified in load file");

  m_cur_line = 1;

  return;
}


/**
 * 
 */
bool LoadDataSource::next(uint32_t *type_flagp, uint64_t *timestampp, KeySpec *keyp, uint8_t **valuep, uint32_t *value_lenp, uint32_t *consumedp) {
  string line;
  char *base, *ptr, *colon, *endptr;

  if (type_flagp)
    *type_flagp = FLAG_INSERT;
  
  if (consumedp)
    *consumedp = 0;

  if (m_hyperformat) {

    while (getline(m_fin, line)) {
      m_cur_line++;

      if (consumedp)
	*consumedp += line.length() + 1;

      m_line_buffer.clear();

      m_line_buffer.add(line.c_str(), strlen(line.c_str())+1);

      base = (char *)m_line_buffer.buf;

      /**
       *  Get timestamp
       */
      if (m_leading_timestamps) {
	if ((ptr = strchr(base, '\t')) == 0) {
	  cerr << "error: too few fields on line " << m_cur_line << endl;
	  continue;
	}
	*ptr++ = 0;
	*timestampp = strtoll(base, &endptr, 10);
	if (*endptr != 0) {
	  cerr << "error: invalid timestamp (" << base << ") on line " << m_cur_line << endl;
	  continue;
	}
	base = ptr;
      }
      else
	*timestampp = 0;

      /**
       * Get row key
       */
      if ((ptr = strchr(base, '\t')) == 0) {
	cerr << "error: too few fields on line " << m_cur_line << endl;
	continue;
      }
      keyp->row = base;
      keyp->row_len = ptr - base;
      *ptr++ = 0;
      base = ptr;

      /**
       * Get column family and qualifier
       */
      if ((ptr = strchr(base, '\t')) == 0) {
	cerr << "error: too few fields on line " << m_cur_line << endl;
	continue;
      }
      *ptr = 0;

      if ((colon = strchr(base, ':')) != 0) {
	*colon++ = 0;
	keyp->column_family = base;
	if (colon < ptr) {
	  keyp->column_qualifier = colon;
	  keyp->column_qualifier_len = ptr - colon;
	}
	else {
	  keyp->column_qualifier = 0;
	  keyp->column_qualifier_len = 0;
	}
      }
      else {
	keyp->column_qualifier = 0;
	keyp->column_qualifier_len = 0;
      }
      keyp->column_family = base;
      ptr++;

      /**
       * Get value
       */
      *valuep = (uint8_t *)ptr;
      *value_lenp = strlen(ptr);

      return true;
    }
  }
  else {

    if (m_next_value > 0 && m_next_value < m_column_names.size()) {
      keyp->row = m_values[0];
      keyp->row_len = m_cur_row_length;
      keyp->column_family = m_column_names[m_next_value].c_str();
      if (keyp->column_qualifier || keyp->column_qualifier_len || *timestampp) {
	keyp->column_qualifier = 0;
	keyp->column_qualifier_len = 0;
	*timestampp = 0;
      }
      if (m_values[m_next_value] == 0) {
	*valuep = 0;
	*value_lenp = 0;
      }
      else {
	*valuep = (uint8_t *)m_values[m_next_value];
	*value_lenp = strlen(m_values[m_next_value]);
      }
      m_next_value++;
      return true;
    }

    while (getline(m_fin, line)) {
      m_cur_line++;

      if (consumedp)
	*consumedp += line.length() + 1;

      m_line_buffer.clear();
      m_line_buffer.add(line.c_str(), strlen(line.c_str())+1);
      m_values.clear();

      base = (char *)m_line_buffer.buf;

      m_next_value = 0;
      while ((ptr = strchr(base, '\t')) != 0) {
	*ptr++ = 0;
	if (strlen(base) == 0 || !strcmp(base, "NULL") || !strcmp(base, "\\N"))
	  m_values.push_back(0);
	else
	  m_values.push_back(base);
	base = ptr;
	m_next_value++;
      }
      m_values.push_back(base);
      m_next_value++;
      
      if (m_next_value != m_column_names.size()) {
	cerr << "error: too few fields on line " << m_cur_line << endl;
	continue;
      }

      if (m_values[0] == 0) {
	cerr << "error: NULL row key on line " << m_cur_line << endl;
	continue;
      }

      m_cur_row_length = strlen(m_values[0]);

      m_next_value = 1;
      keyp->row = m_values[0];
      keyp->row_len = m_cur_row_length;
      keyp->column_family = m_column_names[m_next_value].c_str();
      if (keyp->column_qualifier || keyp->column_qualifier_len || *timestampp) {
	keyp->column_qualifier = 0;
	keyp->column_qualifier_len = 0;
	*timestampp = 0;
      }
      if (m_values[m_next_value] == 0) {
	*valuep = 0;
	*value_lenp = 0;
      }
      else {
	*valuep = (uint8_t *)m_values[m_next_value];
	*value_lenp = strlen(m_values[m_next_value]);
      }
      m_next_value++;

      return true;
    }

  }

  return false;
}
