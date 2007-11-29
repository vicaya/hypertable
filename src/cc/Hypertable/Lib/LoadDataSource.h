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

#ifndef HYPERTABLE_LOADDATASOURCE_H
#define HYPERTABLE_LOADDATASOURCE_H

#include <fstream>
#include <iostream>
#include <string>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"

#include "KeySpec.h"

using namespace Hypertable;

namespace Hypertable {

  class LoadDataSource {

  public:
    LoadDataSource(std::string fname) : m_fin(fname.c_str()), m_cur_line(0), m_line_buffer(0) {
      return;
    }

    bool next(uint64_t *timestampp, KeySpec *keyp, uint8_t **valuep, uint32_t *value_lenp);

  private:
    std::ifstream m_fin;
    long m_cur_line;
    DynamicBuffer m_line_buffer;
  };

}

#endif // HYPERTABLE_LOADDATASOURCE_H
