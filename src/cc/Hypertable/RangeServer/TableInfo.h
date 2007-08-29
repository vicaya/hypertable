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
#ifndef HYPERTABLE_TABLEINFO_H
#define HYPERTABLE_TABLEINFO_H

#include <map>
#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "Common/StringExt.h"

#include "Hypertable/Lib/Types.h"

#include "Range.h"

namespace hypertable {

  class Schema;

  class TableInfo {

  public:
    TableInfo(std::string &name, SchemaPtr &schemaPtr) : mMutex(), mName(name), mSchema(schemaPtr) { return; }
    TableInfo(const char *name, SchemaPtr &schemaPtr) : mMutex(), mName(name), mSchema(schemaPtr) { return; }
    std::string &GetName() { return mName; }
    SchemaPtr &GetSchema() { 
      boost::mutex::scoped_lock lock(mMutex);
      return mSchema; 
    }
    void UpdateSchema(SchemaPtr &schemaPtr) { 
      boost::mutex::scoped_lock lock(mMutex);
      mSchema = schemaPtr;
    }
    bool GetRange(RangeSpecificationT *rangeSpec, RangePtr &rangePtr);
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
