/** -*- c++ -*-
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
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

#ifndef HYPERTABLE_LOAD_DATA_SOURCE_FILE_DFS_H
#define HYPERTABLE_LOAD_DATA_SOURCE_FILE_DFS_H

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <boost/iostreams/filtering_stream.hpp>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"
#include "Common/String.h"

#include "DataSource.h"
#include "FixedRandomStringGenerator.h"
#include "LoadDataSource.h"
#include "DfsBroker/Lib/FileDevice.h"
#include "DfsBroker/Lib/Client.h"

namespace Hypertable {

  class LoadDataSourceFileDfs: public LoadDataSource {

  public:
    LoadDataSourceFileDfs(DfsBroker::ClientPtr &client, const String &fname,
                          const String &header_fname,
                          int row_uniquify_chars = 0, int load_flags = 0);

    ~LoadDataSourceFileDfs() { delete m_source;};

    uint64_t incr_consumed();

  protected:
    void init_src();
    DfsBroker::FileSource *m_source;
    String m_fname;
    String m_header_fname;
    unsigned long m_cur_offset;
  };

} // namespace Hypertable

#endif // HYPERTABLE_LOADDATASOURCEFILELOCAL_H
