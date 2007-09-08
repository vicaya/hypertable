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
    TestSource(std::string &fname, Schema *schema) : mSchema(schema), mFin(fname.c_str()), mCurLine(0), mKeyBuffer(0), mValueBuffer(0) {
      return;
    }

    bool Next(ByteString32T **keyp, ByteString32T **valuep);

  private:
    bool CreateRowDelete(const char *row, uint64_t timestamp, ByteString32T **keyp, ByteString32T **valuep);
    bool CreateColumnDelete(const char *row, const char *column, uint64_t timestamp, ByteString32T **keyp, ByteString32T **valuep);
    bool CreateInsert(const char *row, const char *column, uint64_t timestamp, const char *value, ByteString32T **keyp, ByteString32T **valuep);

    Schema *mSchema;
    ifstream mFin;
    long mCurLine;
    DynamicBuffer mKeyBuffer;
    DynamicBuffer mValueBuffer;
  };

}

