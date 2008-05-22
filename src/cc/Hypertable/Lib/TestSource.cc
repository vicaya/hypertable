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

#include <cerrno>
#include <cstring>

#include <boost/algorithm/string.hpp>
#include <boost/shared_array.hpp>

extern "C" {
#include <strings.h>
}

#include "Common/DynamicBuffer.h"

#include "Key.h"

#include "TestSource.h"

using namespace std;

bool TestSource::next(ByteString &key, ByteString &value) {
  string line;
  boost::shared_array<char> linePtr;
  char *base, *ptr, *last;
  char *rowKey;
  char *column;
  char *value_str;
  uint64_t timestamp;

  while (getline(m_fin, line)) {
    m_cur_line++;

    boost::trim(line);

    linePtr.reset(new char [ strlen(line.c_str()) + 1 ]);
    base = linePtr.get();
    strcpy(base, line.c_str());

    if ((ptr = strtok_r(base, "\t", &last)) == 0) {
      cerr << "Mal-formed input on line " << (m_cur_line-1) << endl;
      continue;
    }

    if (!strcasecmp(ptr, "AUTO")) {
      timestamp = 0;
    }
    else {
      timestamp = strtoll(ptr, 0, 0);
      if (timestamp == 0 && errno == EINVAL) {
	cerr << "Invalid timestamp (" << ptr << ") on line " << (m_cur_line-1) << endl;
	continue;
      }
      if (m_min_timestamp == 0 || timestamp < m_min_timestamp)
	m_min_timestamp = timestamp;
    }

    if ((rowKey = strtok_r(0, "\t", &last)) == 0) {
      cerr << "Mal-formed input on line " << (m_cur_line-1) << endl;
      continue;
    }

    /**
     * If the row key ends in "??", replace the "??" with 0xff 0xff
     */
    size_t row_key_len = strlen(rowKey);
    if (row_key_len >= 2) {
      if (!strcmp(&rowKey[row_key_len-2], "??")) {
	rowKey[row_key_len-1] = (char )0xff;
	rowKey[row_key_len-2] = (char )0xff;
      }
    }

    if ((column = strtok_r(0, "\t", &last)) == 0) {
      cerr << "Mal-formed input on line " << (m_cur_line-1) << endl;
      continue;
    }

    if (!strcmp(column, "DELETE")) {
      if (!create_row_delete(rowKey, timestamp, key, value)) {
	cerr << "Mal-formed input on line " << (m_cur_line-1) << endl;
	continue;
      }
      return true;
    }

    if ((value_str = strtok_r(0, "\t", &last)) == 0)
      value_str = "";

    if (!strcmp(value_str, "DELETE")) {
      if (!create_column_delete(rowKey, column, timestamp, key, value)) {
	cerr << "Mal-formed input on line " << (m_cur_line-1) << endl;
	continue;
      }
      return true;
    }

    row_key_len = strlen(value_str);
    if (row_key_len >= 2) {
      if (!strcmp(&value_str[row_key_len-2], "??")) {
	value_str[row_key_len-1] = (char )0xff;
	value_str[row_key_len-2] = (char )0xff;
	cerr << "converting end of value (" << value_str << ")" << endl;
      }
    }

    if (!create_insert(rowKey, column, timestamp, value_str, key, value)) {
      cerr << "Mal-formed input on line " << (m_cur_line-1) << endl;
      continue;
    }

    return true;
  }

  return false;
}


bool TestSource::create_row_delete(const char *row, uint64_t timestamp, ByteString &key, ByteString &value) {
  int32_t keyLen = strlen(row) + 12;

  m_key_buffer.clear();
  m_key_buffer.ensure(sizeof(int32_t)+keyLen);

  Serialization::encode_vi32(&m_key_buffer.ptr, keyLen);
  m_key_buffer.addNoCheck(row, strlen(row)+1);
  *m_key_buffer.ptr++ = 0;
  *m_key_buffer.ptr++ = 0;
  *m_key_buffer.ptr++ = FLAG_DELETE_ROW;
  Key::encode_ts64(&m_key_buffer.ptr, timestamp);

  key.ptr = m_key_buffer.base;

  m_value_buffer.clear();
  append_as_byte_string(m_value_buffer, 0, 0);
  value.ptr = m_value_buffer.base;
  return true;
}


bool TestSource::create_column_delete(const char *row, const char *column, uint64_t timestamp, ByteString &key, ByteString &value) {
  int32_t keyLen = 0;
  string columnFamily;
  const char *qualifier;
  const char *ptr = strchr(column, ':');
  
  if (ptr == 0) {
    cerr << "Bad column family specifier (no family)" << endl;
    return false;
  }

  columnFamily = string(column, ptr-column);
  qualifier = ptr+1;

  Schema::ColumnFamily *cf = m_schema->get_column_family(columnFamily);
  if (cf == 0) {
    cerr << "Column family '" << columnFamily << "' not found in schema" << endl;
    return false;
  }

  m_key_buffer.clear();
  keyLen = strlen(row) + strlen(qualifier) + 12;
  m_key_buffer.ensure(sizeof(int32_t)+keyLen);

  Serialization::encode_vi32(&m_key_buffer.ptr, keyLen);
  m_key_buffer.addNoCheck(row, strlen(row)+1);
  *m_key_buffer.ptr++ = cf->id;
  m_key_buffer.addNoCheck(qualifier, strlen(qualifier)+1);
  *m_key_buffer.ptr++ = FLAG_DELETE_CELL;
  Key::encode_ts64(&m_key_buffer.ptr, timestamp);

  key.ptr = m_key_buffer.base;

  m_value_buffer.clear();
  append_as_byte_string(m_value_buffer, 0, 0);
  value.ptr = m_value_buffer.base;
  return true;
}


bool TestSource::create_insert(const char *row, const char *column, uint64_t timestamp, const char *value_str, ByteString &key, ByteString &value) {
  int32_t keyLen = 0;
  string columnFamily;
  const char *qualifier;
  const char *ptr = strchr(column, ':');
  
  if (ptr == 0) {
    cerr << "Bad column family specifier (no family)" << endl;
    return false;
  }

  columnFamily = string(column, ptr-column);
  qualifier = ptr+1;

  Schema::ColumnFamily *cf = m_schema->get_column_family(columnFamily);
  if (cf == 0) {
    cerr << "Column family '" << columnFamily << "' not found in schema" << endl;
    return false;
  }

  m_key_buffer.clear();
  keyLen = strlen(row) + strlen(qualifier) + 12;
  m_key_buffer.ensure(keyLen+sizeof(int32_t));

  Serialization::encode_vi32(&m_key_buffer.ptr, keyLen);
  m_key_buffer.addNoCheck(row, strlen(row)+1);
  *m_key_buffer.ptr++ = cf->id;
  m_key_buffer.addNoCheck(qualifier, strlen(qualifier)+1);
  *m_key_buffer.ptr++ = FLAG_INSERT;
  Key::encode_ts64(&m_key_buffer.ptr, timestamp);

  key.ptr = m_key_buffer.base;

  m_value_buffer.clear();
  append_as_byte_string(m_value_buffer, value_str, strlen(value_str));
  value.ptr = m_value_buffer.base;
  return true;
}

