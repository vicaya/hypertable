/**
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

#include "Common/Compat.h"

#include <cstring>

#include "freebase_parser.h"

using namespace std;


freebase_parser::freebase_parser() : m_lineno(0) {
}

/**
 *
 */
bool freebase_parser::load(const std::string fname) {
  ColumnInfo cinfo;

  m_fin.open(fname.c_str());

  m_fname = fname;

  if (!m_fin.is_open()) {
    cerr << "error: problem opening file '" << m_fname << "' for reading"
         << endl;
    return false;
  }

  getline (m_fin, m_header_line);
  m_lineno++;

  m_column_name = -1;
  m_column_id   = -1;

  char *base = (char *)m_header_line.c_str();
  char *ptr = base;
  size_t i=0;

  ptr = strchr(base, '\t');
  while (ptr) {
    *ptr = 0;
    if (!strcmp(base, "name"))
      m_column_name = i;
    else if (!strcmp(base, "id"))
      m_column_id = i;
    cinfo.name = base;
    cinfo.name_len = ptr-base;
    cinfo.value = 0;
    cinfo.value_len = 0;
    m_column_info.push_back(cinfo);
    i++;
    base = ptr+1;
    ptr = strchr(base, '\t');
  }

  if (!strcmp(base, "name"))
    m_column_name = i;
  else if (!strcmp(base, "id"))
    m_column_id = i;
  cinfo.name = base;
  cinfo.name_len = strlen(base);
  cinfo.value = 0;
  cinfo.value_len = 0;
  m_column_info.push_back(cinfo);

  m_inserts = new InsertRec [ m_column_info.size() ];

  if (m_column_name != 0) {
    cerr << "error: 'name' not found in column position 0 in file '"
         << m_fname << "'" << endl;
    return false;
  }

  if (m_column_id != 1) {
    cerr << "error: 'id' not found in column position 1 in file '"
         << m_fname << "'" << endl;
    return false;
  }

  if ((base = (char*)strrchr(fname.c_str(), '/')) == 0)
    base = (char *)fname.c_str();
  else
    base++;
  if ((ptr = strchr(base, '.')) != 0)
    *ptr = 0;
  m_category = base;

  return true;
}




InsertRec *freebase_parser::next(int *countp) {
  char *base, *ptr;
  size_t nfields=0;

  while (!m_fin.eof()) {

    getline(m_fin, m_line);
    m_lineno++;

    base = (char *)m_line.c_str();
    ptr = base;
    ptr = strchr(base, '\t');

    while (ptr) {
      *ptr = 0;
      if (nfields == m_column_info.size())
        break;
      m_column_info[nfields].value = base;
      m_column_info[nfields].value_len = ptr - base;
      nfields++;
      base = ++ptr;
      ptr = strchr(base, '\t');
    }

    if (nfields < m_column_info.size()) {
      m_column_info[nfields].value = base;
      m_column_info[nfields].value_len = strlen(base);
      nfields++;
    }

    if (nfields < 2)
      continue;
    else if (*m_column_info[1].value == 0) {
      cerr << "error: 'id' field not found on line " << m_lineno
           << " of file '" << m_fname << "'" << endl;
      continue;
    }

    const char *row_key = m_column_info[1].value;
    size_t row_key_len = m_column_info[1].value_len;

    // set 'name' column

    size_t j=0;
    if (*m_column_info[0].value != 0) {
      m_inserts[j].key.row = row_key;
      m_inserts[j].key.row_len = row_key_len;
      m_inserts[j].key.column_family = "name";
      m_inserts[j].key.column_qualifier = 0;
      m_inserts[j].key.column_qualifier_len = 0;
      m_inserts[j].value = m_column_info[0].value;
      m_inserts[j].value_len = m_column_info[0].value_len;
      j++;
    }

    // set 'category' column
    m_inserts[j].key.row = row_key;
    m_inserts[j].key.row_len = row_key_len;
    m_inserts[j].key.column_family = "category";
    m_inserts[j].key.column_qualifier = 0;
    m_inserts[j].key.column_qualifier_len = 0;
    m_inserts[j].value = m_category.c_str();
    m_inserts[j].value_len = m_category.length();
    j++;

    for (size_t i=2; i<nfields; i++) {
      if (*m_column_info[i].value) {
        m_inserts[j].key.row = row_key;
        m_inserts[j].key.row_len = row_key_len;
        m_inserts[j].key.column_family = "property";
        m_inserts[j].key.column_qualifier = m_column_info[i].name;
        m_inserts[j].key.column_qualifier_len = m_column_info[i].name_len;
        m_inserts[j].value = m_column_info[i].value;
        m_inserts[j].value_len = m_column_info[i].value_len;
        j++;
      }
    }

    *countp = j;
    return m_inserts;
  }

  return 0;
}
