/** -*- c++ -*-
 * Copyright (C) 2009 Sanjit Jhala (Zvents, Inc.)
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

#ifndef HYPERTABLE_LOADDATASOURCESTDIN_H
#define HYPERTABLE_LOADDATASOURCESTDIN_H

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
#include "FixedRandomStringGenerator.h"
#include "LoadDataSource.h"


namespace Hypertable {

  class LoadDataSourceStdin : public LoadDataSource {

  public:
    LoadDataSourceStdin (const String& header_fname,
                         int row_uniquify_chars = 0, bool dupkeycol = false);

    ~LoadDataSourceStdin() { };

    uint64_t incr_consumed();

  protected:
    String get_header();
    void init_src();

    String m_header_fname;
  };

} // namespace Hypertable

#endif // HYPERTABLE_LOADDATASOURCESTDIN_H
