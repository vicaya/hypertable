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

#include <boost/thread/mutex.hpp>

#include "Common/StringExt.h"
#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/Types.h"

#include "Range.h"

namespace Hypertable {

  class Schema;

  class TableInfo : public ReferenceCount {

  public:
    TableInfo(MasterClientPtr &master_client_ptr, std::string name, SchemaPtr &schemaPtr);
    std::string &get_name() { return m_name; }
    SchemaPtr &get_schema() { 
      boost::mutex::scoped_lock lock(m_mutex);
      return m_schema; 
    }
    void update_schema(SchemaPtr &schemaPtr) { 
      boost::mutex::scoped_lock lock(m_mutex);
      m_schema = schemaPtr;
    }
    bool get_range(RangeT *range, RangePtr &rangePtr);
    void add_range(RangeInfoPtr &rangeInfoPtr);
    bool find_containing_range(std::string row, RangePtr &rangePtr);

  private:

    struct ltEndRow {
      bool operator()(const std::string &s1, const std::string &s2) const {
	if (s1 == "")
	  return false;
	if (s2 == "")
	  return true;
	return s1 < s2;
      }
    };

    typedef std::map<std::string, RangePtr, ltEndRow> RangeMapT;

    boost::mutex    m_mutex;
    MasterClientPtr m_master_client_ptr;
    std::string     m_name;
    SchemaPtr       m_schema;
    RangeMapT       m_range_map;
  };

  typedef boost::intrusive_ptr<TableInfo> TableInfoPtr;

}

#endif // HYPERTABLE_TABLEINFO_H
