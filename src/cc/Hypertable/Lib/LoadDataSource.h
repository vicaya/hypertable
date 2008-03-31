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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_LOADDATASOURCE_H
#define HYPERTABLE_LOADDATASOURCE_H

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"

#include "DataSource.h"

using namespace Hypertable;

namespace Hypertable {

  class LoadDataSource {

  public:
    LoadDataSource(std::string fname, std::string row_key_column, std::string timestamp_column);
    virtual ~LoadDataSource() { return; }
    bool has_timestamps() { return m_leading_timestamps || (m_timestamp_index != -1); }
    virtual bool next(uint32_t *type_flagp, uint64_t *timestampp, KeySpec *keyp, uint8_t **valuep, uint32_t *value_lenp, uint32_t *consumedp);

  private:

    bool parse_date_format(const char *str, struct tm *tm);

    std::vector<std::string> m_column_names;
    std::vector<const char *> m_values;
    size_t m_next_value;
    std::ifstream m_fin;
    long m_cur_line;
    DynamicBuffer m_line_buffer;
    bool m_hyperformat;
    bool m_leading_timestamps;
    size_t m_cur_row_length;
    int m_row_index;
    int m_timestamp_index;
    uint64_t m_timestamp;
    int m_limit;
  };

}

#endif // HYPERTABLE_LOADDATASOURCE_H
