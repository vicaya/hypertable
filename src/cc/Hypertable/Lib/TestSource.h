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

#include <fstream>
#include <iostream>
#include <string>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"
#include "Hypertable/Lib/Schema.h"

using namespace hypertable;

namespace hypertable {

  class TestSource {

  public:
    TestSource(std::string &fname, Schema *schema) : m_schema(schema), m_fin(fname.c_str()), m_cur_line(0), m_key_buffer(0), m_value_buffer(0) {
      return;
    }

    bool next(ByteString32T **keyp, ByteString32T **valuep);

  private:
    bool create_row_delete(const char *row, uint64_t timestamp, ByteString32T **keyp, ByteString32T **valuep);
    bool create_column_delete(const char *row, const char *column, uint64_t timestamp, ByteString32T **keyp, ByteString32T **valuep);
    bool create_insert(const char *row, const char *column, uint64_t timestamp, const char *value, ByteString32T **keyp, ByteString32T **valuep);

    Schema *m_schema;
    ifstream m_fin;
    long m_cur_line;
    DynamicBuffer m_key_buffer;
    DynamicBuffer m_value_buffer;
  };

}

