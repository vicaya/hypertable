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

#ifndef HYPERTABLE_LOADDATASOURCE_H
#define HYPERTABLE_LOADDATASOURCE_H

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filtering_stream.hpp>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"
#include "Common/String.h"

#include "DataSource.h"


namespace Hypertable {

  class LoadDataSource {

  public:
    LoadDataSource(String fname, String header_fname, std::vector<String> &key_columns, String timestamp_column);
    virtual ~LoadDataSource() { delete [] m_go_mask; return; }
    bool has_timestamps() { return m_leading_timestamps || (m_timestamp_index != -1); }
    virtual bool next(uint32_t *type_flagp, uint64_t *timestampp, KeySpec *keyp, uint8_t **valuep, uint32_t *value_lenp, uint32_t *consumedp);

  private:

    class KeyComponentInfo {
    public:
      KeyComponentInfo() : index(0), width(0), left_justify(false), pad_character(' ') { return; }
      void clear() { index=0; width=0; left_justify=false; pad_character=' '; }
      int index;
      int width;
      bool left_justify;
      char pad_character;
    };

    bool parse_date_format(const char *str, struct tm *tm);

    bool add_row_component(int index);

    std::vector<String> m_column_names;
    std::vector<const char *> m_values;
    std::vector<KeyComponentInfo> m_key_comps;
    bool *m_go_mask;
    size_t m_next_value;
    boost::iostreams::filtering_istream m_fin;
    boost::iostreams::file_source m_source;
    long m_cur_line;
    DynamicBuffer m_line_buffer;
    DynamicBuffer m_row_key_buffer;
    bool m_hyperformat;
    bool m_leading_timestamps;
    int m_timestamp_index;
    uint64_t m_timestamp;
    int m_limit;
    uint64_t m_offset;
    bool m_zipped;
  };

}

#endif // HYPERTABLE_LOADDATASOURCE_H
