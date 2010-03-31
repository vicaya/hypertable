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
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
}

#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Time.h"

#include "Key.h"

#include "LoadDataFlags.h"
#include "LoadDataSource.h"

using namespace boost::iostreams;
using namespace Hypertable;
using namespace std;


/**
 *
 */
LoadDataSource::LoadDataSource(const String &header_fname, int row_uniquify_chars, int load_flags)
  : m_type_mask(0), m_cur_line(0), m_line_buffer(0),
    m_row_key_buffer(0), m_hyperformat(false), m_leading_timestamps(false),
    m_timestamp_index(-1), m_timestamp(AUTO_ASSIGN), m_offset(0),
    m_zipped(false), m_rsgen(0), m_header_fname(header_fname), 
    m_row_uniquify_chars(row_uniquify_chars),
    m_load_flags(load_flags), m_first_line_cached(false) {
  if (row_uniquify_chars)
    m_rsgen = new FixedRandomStringGenerator(row_uniquify_chars);

  return;
}

String
LoadDataSource::get_header()
{
  String header = "";
  if (m_header_fname != "") {
    std::ifstream in(m_header_fname.c_str());
    getline(in, header);
  }
  else if (LoadDataFlags::single_cell_format(m_load_flags)) {
    // force autodetect
    size_t tabs = 0;
    getline(m_fin, m_first_line);
    for (const char *ptr = m_first_line.c_str(); *ptr; ptr++) {
      if (*ptr == '\t')
	tabs++;
    }
    if (tabs == 2) {
      if (strcmp(m_first_line.c_str(), "#row\tcolumn\tvalue"))
	m_first_line_cached = true;
      header = "#row\tcolumn\tvalue";
    }
    else if (tabs == 3) {
      if (strcmp(m_first_line.c_str(), "#timestamp\trow\tcolumn\tvalue"))
	m_first_line_cached = true;
      header = "#timestamp\trow\tcolumn\tvalue";
    }
    else
      HT_THROWF(Error::HQL_BAD_LOAD_FILE_FORMAT,
		"Untable to autodetect format, expected 2 or 3 tabs, got %d", (int)tabs);
  }
  else {
    // autodetect
    getline(m_fin, m_first_line);
    if (m_first_line[0] == '#')
      header = m_first_line;
    else {
      size_t tabs = 0;
      for (const char *ptr = m_first_line.c_str(); *ptr; ptr++) {
	if (*ptr == '\t')
	  tabs++;
      }
      if (tabs == 2)
	header = "#row\tcolumn\tvalue";
      else if (tabs == 3)
	header = "#timestamp\trow\tcolumn\tvalue";
      else
	HT_THROWF(Error::HQL_BAD_LOAD_FILE_FORMAT,
		  "Untable to autodetect format, expected 2 or 3 tabs, got %d", (int)tabs);
      m_first_line_cached = true;
    }
  }

  return header;
}



void
LoadDataSource::init(const std::vector<String> &key_columns, const String &timestamp_column)
{
  String header;
  init_src();
  header = get_header();
  parse_header(header, key_columns, timestamp_column);
}

void
LoadDataSource::parse_header(const String &header, const std::vector<String> &key_columns,
                             const String &timestamp_column) {

  String line, column_name;
  char *base, *ptr, *colon_ptr;
  int index = 0;
  KeyComponentInfo key_comps;
  ColumnInfo cinfo;

  // Assuming max column size of 256
  m_type_mask = new uint32_t [257];
  memset(m_type_mask, 0, 257*sizeof(uint32_t));

  base = (char *)header.c_str();
  if (*base == '#') {
    base++;
    while (isspace(*base))
      base++;
  }

  ptr = strchr(base, '\t');

  while (base) {

    if (ptr)
      *ptr++ = 0;

    colon_ptr = strchr(base, ':');
    if (colon_ptr) {
      cinfo.family = String(base, colon_ptr - base);
      cinfo.qualifier = colon_ptr+1;
    }
    else {
      cinfo.family = base;
      cinfo.qualifier = "";
    }

    m_column_info.push_back(cinfo);

    if (timestamp_column != "" && timestamp_column == cinfo.family) {
      m_timestamp_index = index;
      m_type_mask[index] |= TIMESTAMP;
    }

    index++;
    HT_EXPECT(index < 256, Error::TOO_MANY_COLUMNS);

    if (ptr) {
      base = ptr;
      ptr = strchr(base, '\t');
    }
    else
      base = 0;
  }


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

    for (j=0; j<m_column_info.size(); j++) {
      if (m_column_info[j].family == column_name) {
        key_comps.index = j;
        m_key_comps.push_back(key_comps);
        m_type_mask[j] |= ROW_KEY;
        break;
      }
    }
    if (j == m_column_info.size()) {
      cout << "ERROR: key column '" << column_name
           << "' not found in input file" << endl;
      exit(1);
    }
  }

  if (m_key_comps.empty()) {
    key_comps.clear();
    m_key_comps.push_back(key_comps);
    m_type_mask[0] |= ROW_KEY;
  }

  if (m_column_info.size() == 3 || m_column_info.size() == 4) {
    size_t i=m_column_info.size()-3;
    if ((m_column_info[i].family == "rowkey" ||
         m_column_info[i].family == "row") &&
        (m_column_info[i+1].family == "columnkey" ||
         m_column_info[i+1].family == "column") &&
        m_column_info[i+2].family == "value") {
      if (i == 0 || m_column_info[0].family == "timestamp") {
        m_hyperformat = true;
        m_leading_timestamps = (i==1);
      }
    }
  }

  m_next_value = m_column_info.size();
  m_limit = 0;

  if (!m_hyperformat && m_column_info.size() < 2)
    HT_THROW(Error::HQL_BAD_LOAD_FILE_FORMAT,
             "No columns specified in load file");

  m_cur_line = 1;
}

/**
 *
 */
bool
LoadDataSource::next(uint32_t *type_flagp, KeySpec *keyp,
    uint8_t **valuep, uint32_t *value_lenp, uint32_t *consumedp) {
  String line;
  int index;
  char *base, *ptr, *colon;

  if (type_flagp)
    *type_flagp = FLAG_INSERT;

  if (consumedp)
    *consumedp = 0;

  if (m_hyperformat) {

    while (get_next_line(line)) {
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
          cerr << "warning: too few fields on line " << m_cur_line << endl;
          continue;
        }
        *ptr++ = 0;

        int64_t timestamp;
        if (!parse_date_format(base, timestamp)) {
          cerr << "warn: invalid timestamp format on line " << m_cur_line
               << ", skipping..." << endl;
          continue;
        }
        keyp->timestamp = (int64_t)timestamp;

        base = ptr;
      }
      else
        keyp->timestamp = AUTO_ASSIGN;

      /**
       * Get row key
       */
      if ((ptr = strchr(base, '\t')) == 0) {
        cerr << "warning: too few fields on line " << m_cur_line << endl;
        continue;
      }
      if (m_rsgen) {
        keyp->row_len = (ptr-base) + m_row_uniquify_chars + 1;
        m_row_key_buffer.clear();
        m_row_key_buffer.ensure(keyp->row_len + 1);
        m_row_key_buffer.add_unchecked(base, ptr-base);
        m_row_key_buffer.add_unchecked(" ", 1);
        m_rsgen->write((char *)m_row_key_buffer.ptr);
        keyp->row = m_row_key_buffer.base;
      }
      else {
        keyp->row = base;
        keyp->row_len = ptr - base;
      }
      *ptr++ = 0;
      base = ptr;

      /**
       * Get column family and qualifier
       */
      if ((ptr = strchr(base, '\t')) == 0) {
        cerr << "warning: too few fields on line " << m_cur_line << endl;
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
        *consumedp = incr_consumed();
      }

      return true;
    }
  }
  else {

    // skip timestamp and rowkey (if needed)
    while (m_next_value < m_limit &&
           should_skip(m_next_value, m_type_mask))
      m_next_value++;

    // get from parsed cells if available
    if (m_next_value > 0 && m_next_value < m_limit) {
      keyp->row = m_row_key_buffer.base;
      keyp->row_len = m_row_key_buffer.fill();
      keyp->column_family = m_column_info[m_next_value].family.c_str();
      keyp->timestamp = m_timestamp;

      // clear these, just in case they were set by the client
      if (m_column_info[m_next_value].qualifier.empty()) {
        keyp->column_qualifier = 0;
        keyp->column_qualifier_len = 0;
      }
      else {
        keyp->column_qualifier = m_column_info[m_next_value].qualifier.c_str();
        keyp->column_qualifier_len =
            m_column_info[m_next_value].qualifier.length();
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
        *consumedp = incr_consumed();
      }

      return true;
    }

    while (get_next_line(line)) {
      m_cur_line++;
      index = 0;

      if (consumedp && !m_zipped)
        *consumedp += line.length() + 1;

      boost::trim(line);
      if (line.length() == 0)
        continue;

      m_line_buffer.clear();
      m_line_buffer.add(line.c_str(), strlen(line.c_str())+1);
      m_values.clear();

      base = (char *)m_line_buffer.base;

      while ((ptr = strchr(base, '\t')) != 0) {
        *ptr++ = 0;

        if (strlen(base) == 0 || !strcmp(base, "NULL") ||
            !strcmp(base, "\\N")) {
          if (m_type_mask[index]) {
            cout << "WARNING: Required key or timestamp field not found "
                "on line " << m_cur_line << ", skipping ..." << endl << flush;
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

      m_limit = std::min(m_values.size(), m_column_info.size());

      /**
       * setup row key
       */
      m_row_key_buffer.clear();
      if (!add_row_component(0))
        continue;

      size_t i;
      for (i=1; i<m_key_comps.size(); i++) {
        m_row_key_buffer.add(" ", 1);
        if (!add_row_component(i))
          break;
      }
      if (i<m_key_comps.size())
        continue;       // rowkey not found. warning in add_row_component.

      m_row_key_buffer.ensure(1);
      *m_row_key_buffer.ptr = 0;

      if (m_timestamp_index >= 0) {

        if (m_values.size() <= (size_t)m_timestamp_index) {
          cerr << "warn: timestamp field not found on line " << m_cur_line
               << ", skipping..." << endl;
          continue;
        }

        if (!parse_date_format(m_values[m_timestamp_index], m_timestamp)) {
          cerr << "warn: invalid timestamp format on line " << m_cur_line
               << ", skipping..." << endl;
          continue;
        }
      }
      else
        m_timestamp = AUTO_ASSIGN;

      m_next_value = 0;
      while (should_skip(m_next_value, m_type_mask))
        m_next_value++;

      if (m_rsgen) {
        m_row_key_buffer.ensure(m_row_uniquify_chars + 2);
        keyp->row = m_row_key_buffer.base;
        keyp->row_len = m_row_key_buffer.fill() + m_row_uniquify_chars + 1;
        m_row_key_buffer.add_unchecked(" ", 1);
        m_rsgen->write((char *)m_row_key_buffer.ptr);
        m_row_key_buffer.ptr += m_row_uniquify_chars;
      }
      else {
        keyp->row = m_row_key_buffer.base;
        keyp->row_len = m_row_key_buffer.fill();
      }

      keyp->column_family = m_column_info[m_next_value].family.c_str();
      keyp->timestamp = m_timestamp;

      if (m_column_info[m_next_value].qualifier.empty()) {
        keyp->column_qualifier = 0;
        keyp->column_qualifier_len = 0;
      }
      else {
        keyp->column_qualifier = m_column_info[m_next_value].qualifier.c_str();
        keyp->column_qualifier_len =
            m_column_info[m_next_value].qualifier.length();
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
        *consumedp = incr_consumed();
      }

      return true;
    }

  }

  return false;
}



bool LoadDataSource::add_row_component(int index) {
  const char *value = m_values[m_key_comps[index].index];
  size_t value_len = 0;

  if (value == 0) {
    cout << "WARNING: Required key field not found on line " << m_cur_line
         << ", skipping ..." << endl << flush;
    return false;
  }

  value_len = strlen(value);

  if ((size_t)m_key_comps[index].index >= m_values.size() || value == 0) {
    cout << "WARNING: Required key field not found on line " << m_cur_line
         << ", skipping ..." << endl << flush;
    return false;
  }

  if ((size_t)m_key_comps[index].width > value_len) {
    size_t padding = m_key_comps[index].width - value_len;
    m_row_key_buffer.ensure(m_key_comps[index].width);
    if (m_key_comps[index].left_justify) {
      m_row_key_buffer.add(value, value_len);
      memset(m_row_key_buffer.ptr, m_key_comps[index].pad_character, padding);
      m_row_key_buffer.ptr += padding;
    }
    else {
      memset(m_row_key_buffer.ptr, m_key_comps[index].pad_character, padding);
      m_row_key_buffer.ptr += padding;
      m_row_key_buffer.add(value, value_len);
    }
  }
  else
    m_row_key_buffer.add(value, value_len);

  return true;

}

bool LoadDataSource::parse_date_format(const char *str, int64_t &timestamp) {
  int ival;
  double dval=0;
  const char *ptr = str;
  char *end_ptr;
  struct tm tm;
  time_t tt;
  int64_t ns;

  ns = (int64_t)strtoll(ptr, &end_ptr, 10);
  if (*end_ptr == 0) {
    timestamp = ns;
    return true;
  }
  else if ((end_ptr - ptr) != 4)
    return false;

  tm.tm_year = ns - 1900;
  ptr = end_ptr + 1;
  ns = 0;

  /**
   * month
   */
  if ((ival = strtol(ptr, &end_ptr, 10)) == 0 || (end_ptr - ptr) != 2 ||
      *end_ptr != '-')
    return false;

  tm.tm_mon = ival - 1;
  ptr = end_ptr + 1;

  /**
   * day
   */
  if ((ival = strtol(ptr, &end_ptr, 10)) == 0 || (end_ptr - ptr) != 2 ||
      (*end_ptr != ' ' && *end_ptr != 'T'))
    return false;

  tm.tm_mday = ival;
  ptr = end_ptr + 1;

  /**
   * hour
   */
  ival = strtol(ptr, &end_ptr, 10);
  if ((end_ptr - ptr) != 2 || *end_ptr != ':')
    return false;
  tm.tm_hour = ival;

  ptr = end_ptr + 1;

  /**
   * minute
   */
  ival = strtol(ptr, &end_ptr, 10);
  if ((end_ptr - ptr) != 2 || *end_ptr != ':')
    return false;
  tm.tm_min = ival;

  ptr = end_ptr + 1;

  /**
   * second
   */
  dval = strtod(ptr, &end_ptr);
  tm.tm_sec = 0;

#if !defined(__sun__)
  tm.tm_gmtoff = 0;
  tm.tm_zone = (char *)"GMT";
#endif

  if ((tt = timegm(&tm)) == (time_t)-1)
    return false;

  ptr = end_ptr + 1;
  // add integer nanoseconds
  if (*end_ptr == ':') {
    ns = strtoul(ptr, &end_ptr, 10);
  }

  timestamp = (int64_t)(((double)tt + dval) * 1000000000LL) + ns;

  return true;
}
