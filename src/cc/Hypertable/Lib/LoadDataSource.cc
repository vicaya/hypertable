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

#include <fstream>
#include <iostream>
#include <cerrno>
#include <cctype>
#include <cstdlib>
#include <cstring>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/shared_array.hpp>

extern "C" {
#include <strings.h>
#include <time.h>
}

#include "Common/ByteOrder.h"
#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"

#include "Key.h"

#include "LoadDataSource.h"

using namespace boost::iostreams;
using namespace Hypertable;
using namespace std;

/**
 *
 */
LoadDataSource::LoadDataSource(String fname, String header_fname, std::vector<String> &key_columns, String timestamp_column) : m_go_mask(0), m_source(fname), m_cur_line(0), m_line_buffer(0), m_row_key_buffer(0), m_hyperformat(false), m_leading_timestamps(false), m_timestamp_index(-1), m_timestamp(0), m_offset(0), m_zipped(false) {
  string line, column_name;
  char *base, *ptr;
  int index = 0;
  KeyComponentInfo key_comps;

  if (boost::algorithm::ends_with(fname, ".gz")) {
    m_fin.push(gzip_decompressor());
    m_zipped = true;
  }
  m_fin.push(m_source);

  if (header_fname != "") {
    std::ifstream in(header_fname.c_str());
    if (!getline(in, line))
      return;
  }
  else {
    if (!getline(m_fin, line))
      return;
  }

  m_go_mask = new bool [ 257 ];
  memset(m_go_mask, true, 257*sizeof(bool));

  base = (char *)line.c_str();
  if (*base == '#') {
    base++;
    while (isspace(*base))
      base++;
  }
  while ((ptr = strchr(base, '\t')) != 0) {
    *ptr++ = 0;
    m_column_names.push_back(base);
    column_name = String(base);
    if (timestamp_column != "" && timestamp_column == column_name) {
      m_timestamp_index = index;
      m_go_mask[index] = false;
    }
    base = ptr;
    index++;
    HT_EXPECT(index < 256, Error::ASSERTION_FAILURE);
  }
  m_column_names.push_back(base);

  /**
   * Set up row key columns
   */
  for (size_t i=0; i<key_columns.size(); i++) {
    size_t j;
    const char *ptr;

    key_comps.clear();

    if (boost::algorithm::starts_with(key_columns[i], "\\%"))
      column_name = key_columns[i].substr(1);
    else if (boost::algorithm::starts_with(key_columns[i], "%0")) {
      key_comps.pad_character = '0';
      column_name = key_columns[i].substr(2);
      ptr = column_name.c_str();
      key_comps.width = atoi(ptr);
      while (isdigit(*ptr))
	ptr++;
      column_name = String(ptr);
    }
    else if (boost::algorithm::starts_with(key_columns[i], "%-")) {
      key_comps.left_justify = true;
      column_name = key_columns[i].substr(2);
      ptr = column_name.c_str();
      key_comps.width = atoi(ptr);
      while (isdigit(*ptr))
	ptr++;
      column_name = String(ptr);
    }
    else if (boost::algorithm::starts_with(key_columns[i], "%")) {
      column_name = key_columns[i].substr(1);
      ptr = column_name.c_str();
      key_comps.width = atoi(ptr);
      while (isdigit(*ptr))
	ptr++;
      column_name = String(ptr);
    }
    else
      column_name = key_columns[i];

    for (j=0; j<m_column_names.size(); j++) {
      if (m_column_names[j] == column_name) {
	key_comps.index = j;
	m_key_comps.push_back(key_comps);
	m_go_mask[j] = false;
	break;
      }
    }
    if (j == m_column_names.size()) {
      cout << "ERROR: key column '" << column_name << "' not found in input file" << endl;
      exit(1);
    }
  }

  if (m_key_comps.empty()) {
    key_comps.clear();
    m_key_comps.push_back(key_comps);
    m_go_mask[0] = false;
  }

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

  m_next_value = m_column_names.size();
  m_limit = 0;

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
  int index;
  char *base, *ptr, *colon, *endptr;

  if (type_flagp)
    *type_flagp = FLAG_INSERT;
  
  if (consumedp)
    *consumedp = 0;

  if (m_hyperformat) {

    while (getline(m_fin, line)) {
      m_cur_line++;

      if (consumedp && !m_zipped)
	*consumedp += line.length() + 1;

      m_line_buffer.clear();

      m_line_buffer.add(line.c_str(), strlen(line.c_str())+1);

      base = (char *)m_line_buffer.base;

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

      if (m_zipped && consumedp) {
	uint64_t new_offset = m_source.seek(0, BOOST_IOS::cur);
	*consumedp = new_offset - m_offset;
	m_offset = new_offset;
      }

      return true;
    }
  }
  else {

    while (!m_go_mask[m_next_value])
      m_next_value++;

    if (m_next_value > 0 && m_next_value < (size_t)m_limit) {
      keyp->row = m_row_key_buffer.base;
      keyp->row_len = m_row_key_buffer.fill();
      keyp->column_family = m_column_names[m_next_value].c_str();
      *timestampp = m_timestamp;
      // clear these, just in case they were set by the client
      if (keyp->column_qualifier || keyp->column_qualifier_len) {
	keyp->column_qualifier = 0;
	keyp->column_qualifier_len = 0;
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

      if (m_zipped && consumedp) {
	uint64_t new_offset = m_source.seek(0, BOOST_IOS::cur);
	*consumedp = new_offset - m_offset;
	m_offset = new_offset;
      }

      return true;
    }

    while (getline(m_fin, line)) {
      m_cur_line++;
      index = 0;

      if (consumedp && !m_zipped)
	*consumedp += line.length() + 1;

      m_line_buffer.clear();
      m_line_buffer.add(line.c_str(), strlen(line.c_str())+1);
      m_values.clear();

      base = (char *)m_line_buffer.base;

      while ((ptr = strchr(base, '\t')) != 0) {
	*ptr++ = 0;
	if (strlen(base) == 0 || !strcmp(base, "NULL") || !strcmp(base, "\\N")) {
	  if (!m_go_mask[index]) {
	    cout << "WARNING: Required key or timestamp field not found on line " << m_cur_line << ", skipping ..." << endl << flush;
	    continue;
	  }
	  m_values.push_back(0);
	}
	else
	  m_values.push_back(base);
	base = ptr;
	index++;
      }
      if (strlen(base) == 0 || !strcmp(base, "NULL") || !strcmp(base, "\\N"))
	m_values.push_back(0);
      else
	m_values.push_back(base);
      m_limit = std::min(m_values.size(), m_column_names.size());

      /**
       * setup row key
       */
      m_row_key_buffer.clear();
      if (!add_row_component(0))
	continue;
      size_t i;
      for (i=1; i<m_key_comps.size(); i++) {
	m_row_key_buffer.add( " ", 1 );
	if (!add_row_component(i))
	  break;
      }
      if (i<m_key_comps.size())
	continue;
      m_row_key_buffer.ensure(1);
      *m_row_key_buffer.ptr = 0;

      if (m_timestamp_index >= 0) {
	struct tm tm;
	time_t t;

	if (m_values.size() <= (size_t)m_timestamp_index) {
	  cerr << "warn: timestamp field not found on line " << m_cur_line << ", skipping..." << endl;
	  continue;
	}	

	if (!parse_date_format(m_values[m_timestamp_index], &tm)) {
	  cerr << "warn: invalid timestamp format on line " << m_cur_line << ", skipping..." << endl;
	  continue;
	}

	if ((t = timegm(&tm)) == (time_t)-1) {
	  cerr << "warn: invalid timestamp format on line " << m_cur_line << ", skipping..." << endl;
	  continue;
	}

	m_timestamp = (uint64_t)t * 1000000000LL;
      }

      m_next_value = 0;
      while (!m_go_mask[m_next_value])
	m_next_value++;

      keyp->row = m_row_key_buffer.base;
      keyp->row_len = m_row_key_buffer.fill();
      keyp->column_family = m_column_names[m_next_value].c_str();
      *timestampp = m_timestamp;
      if (keyp->column_qualifier || keyp->column_qualifier_len) {
	keyp->column_qualifier = 0;
	keyp->column_qualifier_len = 0;
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

      if (m_zipped && consumedp) {
	uint64_t new_offset = m_source.seek(0, BOOST_IOS::cur);
	*consumedp = new_offset - m_offset;
	m_offset = new_offset;
      }

      return true;
    }

  }

  return false;
}



bool LoadDataSource::add_row_component(int index) {
  const char *value = m_values[m_key_comps[index].index];
  size_t value_len = strlen(value);

  if ((size_t)m_key_comps[index].index >= m_values.size() || value == 0) {
    cout << "WARNING: Required key field not found on line " << m_cur_line << ", skipping ..." << endl << flush;
    return false;
  }

  if ((size_t)m_key_comps[index].width > value_len) {
    size_t padding = m_key_comps[index].width - value_len;
    m_row_key_buffer.ensure( m_key_comps[index].width );
    if (m_key_comps[index].left_justify) {
      m_row_key_buffer.add( value, value_len );
      memset(m_row_key_buffer.ptr, m_key_comps[index].pad_character, padding);
      m_row_key_buffer.ptr += padding;
    }
    else {
      memset(m_row_key_buffer.ptr, m_key_comps[index].pad_character, padding);
      m_row_key_buffer.ptr += padding;
      m_row_key_buffer.add( value, value_len );
    }
  }
  else
    m_row_key_buffer.add( value, value_len );

  return true;
  
}



bool LoadDataSource::parse_date_format(const char *str, struct tm *tm) {
  int ival;
  const char *ptr = str;
  char *end_ptr;

  /**
   * year
   */
  if ((ival = strtol(ptr, &end_ptr, 10)) == 0 || (end_ptr - ptr) != 4 || *end_ptr != '-')
    return false;
  tm->tm_year = ival - 1900;

  ptr = end_ptr + 1;

  /**
   * month
   */
  if ((ival = strtol(ptr, &end_ptr, 10)) == 0 || (end_ptr - ptr) != 2 || *end_ptr != '-')
    return false;
  tm->tm_mon = ival - 1;

  ptr = end_ptr + 1;
  
  /**
   * day
   */
  if ((ival = strtol(ptr, &end_ptr, 10)) == 0 || (end_ptr - ptr) != 2 || *end_ptr != ' ')
    return false;
  tm->tm_mday = ival;

  ptr = end_ptr + 1;

  /**
   * hour
   */
  ival = strtol(ptr, &end_ptr, 10);
  if ((end_ptr - ptr) != 2 || *end_ptr != ':')
    return false;
  tm->tm_hour = ival;

  ptr = end_ptr + 1;

  /**
   * minute
   */
  ival = strtol(ptr, &end_ptr, 10);
  if ((end_ptr - ptr) != 2 || *end_ptr != ':')
    return false;
  tm->tm_min = ival;

  ptr = end_ptr + 1;

  /**
   * second
   */
  ival = strtol(ptr, &end_ptr, 10);
  if ((end_ptr - ptr) != 2)
    return false;
  tm->tm_sec = ival;

  tm->tm_gmtoff = 0;
  tm->tm_zone = "GMT";

  return true;
}
