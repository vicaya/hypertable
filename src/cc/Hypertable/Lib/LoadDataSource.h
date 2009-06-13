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
#include "Common/ReferenceCount.h"

#include "DataSource.h"
#include "FixedRandomStringGenerator.h"


namespace Hypertable {

  enum {
    LOCAL_FILE = 1,
    DFS_FILE,
    STDIN
  };

  class LoadDataSource : public ReferenceCount {

  public:
    LoadDataSource(int row_uniquify_chars = 0,
                   bool dupkeycol = false);

    virtual ~LoadDataSource() { delete [] m_type_mask; return; }

    bool has_timestamps() {
      return m_leading_timestamps || (m_timestamp_index != -1);
    }

    virtual bool next(uint32_t *type_flagp, KeySpec *keyp,
                      uint8_t **valuep, uint32_t *value_lenp,
                      uint32_t *consumedp);

    virtual void init(const std::vector<String> &key_columns, const String &timestamp_column);

    int64_t get_current_lineno() { return m_cur_line; }

  protected:

    virtual void parse_header(const String& header,
                              const std::vector<String> &key_columns,
                              const String &timestamp_column);
    virtual void init_src()=0;
    virtual String get_header()=0;
    virtual uint64_t incr_consumed()=0;

    virtual bool should_skip(int idx, const uint32_t *masks, bool dupkeycols) {
      uint32_t bm = masks[idx];
      return bm && ((bm & TIMESTAMP) || !(dupkeycols && (bm & ROW_KEY)));
    }

    class KeyComponentInfo {
    public:
      KeyComponentInfo()
          : index(0), width(0), left_justify(false), pad_character(' ') {}
      void clear() { index=0; width=0; left_justify=false; pad_character=' '; }
      int index;
      int width;
      bool left_justify;
      char pad_character;
    };

    enum TypeMask {
      ROW_KEY =           (1 << 0),
      TIMESTAMP =         (1 << 1)
    };

    bool parse_date_format(const char *str, struct tm *tm);

    bool add_row_component(int index);

    struct ColumnInfo {
      String family;
      String qualifier;
    };

    std::vector<ColumnInfo> m_column_info;
    std::vector<const char *> m_values;
    std::vector<KeyComponentInfo> m_key_comps;
    uint32_t *m_type_mask;
    size_t m_next_value;
    boost::iostreams::filtering_istream m_fin;
    int64_t m_cur_line;
    DynamicBuffer m_line_buffer;
    DynamicBuffer m_row_key_buffer;
    bool m_hyperformat;
    bool m_leading_timestamps;
    int m_timestamp_index;
    uint64_t m_timestamp;
    size_t m_limit;
    uint64_t m_offset;
    bool m_zipped;
    FixedRandomStringGenerator *m_rsgen;
    int m_row_uniquify_chars;
    bool m_dupkeycols;
  };

 typedef boost::intrusive_ptr<LoadDataSource> LoadDataSourcePtr;

} // namespace Hypertable

#endif // HYPERTABLE_LOADDATASOURCE_H
