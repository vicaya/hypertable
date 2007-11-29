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

#ifndef HYPERTABLE_MUTATORSCATTERBUFFER_H
#define HYPERTABLE_MUTATORSCATTERBUFFER_H

#include <vector>

#include "Common/ByteString.h"
#include "Common/ReferenceCount.h"
#include "Common/StringExt.h"

#include "RangeLocator.h"
#include "RangeServerClient.h"
#include "Schema.h"

namespace Hypertable {

  class MutatorScatterBuffer : public ReferenceCount {
  public:
    MutatorScatterBuffer(ConnectionManagerPtr &conn_manager_ptr, TableIdentifierT *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr);
    void set(uint64_t timestamp, KeySpec &key, uint8_t *value, uint32_t value_len);
    int flush();
    bool wait_for_completion();

  private:

    class UpdateBuffer : public ReferenceCount {
    public:
      std::vector<ByteString32T *>  keys;
      DynamicBuffer                 buf;
    };
    typedef boost::intrusive_ptr<UpdateBuffer> UpdateBufferPtr;

    typedef __gnu_cxx::hash_map<string, UpdateBufferPtr> UpdateBufferMapT;

    ConnectionManagerPtr m_conn_manager_ptr;
    SchemaPtr            m_schema_ptr;
    RangeLocatorPtr      m_range_locator_ptr;
    RangeServerClient    m_range_server;
    std::string          m_table_name;
    TableIdentifierT     m_table_identifier;
    UpdateBufferMapT     m_buffer_map;
  };
  typedef boost::intrusive_ptr<MutatorScatterBuffer> MutatorScatterBufferPtr;


}

#endif // HYPERTABLE_MUTATORSCATTERBUFFER_H
