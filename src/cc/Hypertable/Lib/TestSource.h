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

#ifndef HYPERTABLE_TESTSOURCE_H
#define HYPERTABLE_TESTSOURCE_H

#include <fstream>
#include <iostream>
#include <string>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"
#include "Hypertable/Lib/Schema.h"


namespace Hypertable {

  class TestSource {

  public:
    TestSource(const String &fname, Schema *schema) : m_schema(schema), m_fin(fname.c_str()), m_cur_line(0), m_key_buffer(0), m_value_buffer(0), m_min_timestamp(0) {
      return;
    }

    bool next(ByteString &key, ByteString &value);
    void clear_min_timestamp() { m_min_timestamp = 0; }
    int64_t get_min_timestamp() { return m_min_timestamp; }

  private:
    bool create_row_delete(const char *row, int64_t timestamp, ByteString &key, ByteString &value);
    bool create_column_delete(const char *row, const char *column, int64_t timestamp, ByteString &key, ByteString &value);
    bool create_insert(const char *row, const char *column, int64_t timestamp, const char *value_str, ByteString &key, ByteString &value);

    Schema *m_schema;
    std::ifstream m_fin;
    long m_cur_line;
    DynamicBuffer m_key_buffer;
    DynamicBuffer m_value_buffer;
    int64_t m_min_timestamp;
  };

}

#endif // HYPERTABLE_TESTSOURCE_H
