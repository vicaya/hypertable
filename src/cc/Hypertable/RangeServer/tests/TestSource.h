/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include <fstream>
#include <iostream>
#include <string>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"
#include "Hypertable/Schema.h"
#include "Hypertable/RangeServer/Key.h"

using namespace hypertable;
using namespace hypertable;

class TestSource {
  
 public:
  TestSource(std::string &fname, Schema *schema) : mSchema(schema), mFin(fname.c_str()), mCurLine(0), mKeyBuffer(0), mValueBuffer(0) {
    return;
  }

  bool Next(KeyT **keyp, ByteString32T **valuep);

 private:
  bool CreateRowDelete(const char *row, uint64_t timestamp, KeyT **keyp, ByteString32T **valuep);
  bool CreateColumnDelete(const char *row, const char *column, uint64_t timestamp, KeyT **keyp, ByteString32T **valuep);
  bool CreateInsert(const char *row, const char *column, uint64_t timestamp, const char *value, KeyT **keyp, ByteString32T **valuep);

  Schema *mSchema;
  ifstream mFin;
  long mCurLine;
  DynamicBuffer mKeyBuffer;
  DynamicBuffer mValueBuffer;
};


