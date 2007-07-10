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
#ifndef HYPERTABLE_TABLEINFO_H
#define HYPERTABLE_TABLEINFO_H

#include <map>

#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "Common/StringExt.h"

#include "Range.h"

namespace hypertable {

  class Schema;

  class TableInfo {

  public:
    TableInfo(std::string &name, SchemaPtr &schemaPtr) : mMutex(), mName(name), mSchema(schemaPtr) { return; }
    std::string &GetName() { return mName; }
    SchemaPtr &GetSchema() { 
      boost::mutex::scoped_lock lock(mMutex);
      return mSchema; 
    }
    void UpdateSchema(SchemaPtr &schemaPtr) { 
      boost::mutex::scoped_lock lock(mMutex);
      mSchema = schemaPtr;
    }
    bool GetRange(string &startRow, RangePtr &rangePtr);
    void AddRange(RangeInfoPtr &rangeInfoPtr);

  private:
    typedef std::map<std::string, RangePtr> RangeMapT;

    boost::mutex   mMutex;
    std::string    mName;
    SchemaPtr      mSchema;
    RangeMapT      mRangeMap;
  };

  typedef boost::shared_ptr<TableInfo> TableInfoPtr;

}

#endif // HYPERTABLE_TABLEINFO_H
