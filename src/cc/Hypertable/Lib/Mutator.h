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

#ifndef HYPERTABLE_MUTATOR_H
#define HYPERTABLE_MUTATOR_H

#include "AsyncComm/ConnectionManager.h"

#include "Common/StringExt.h"
#include "Common/ReferenceCount.h"

#include "Cell.h"
#include "KeySpec.h"
#include "MutatorScatterBuffer.h"
#include "RangeLocator.h"
#include "RangeServerClient.h"
#include "Schema.h"
#include "Types.h"

namespace Hypertable {

  class Mutator : public ReferenceCount {

  public:
    Mutator(ConnectionManagerPtr &conn_manager_ptr, TableIdentifierT *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr);
    virtual ~Mutator() { return; }

    void set(uint64_t timestamp, KeySpec &key, uint8_t *value, uint32_t value_len);
    void flush();

    /**
    void delete_row(uint64_t timestamp, KeySpec &key);
    void delete_cell(uint64_t timestamp, KeySpec &key);
    void delete_qualified_cell(uint64_t timestamp, KeySpec &key);
    **/

    //void flush(MutationResultPtr &resultPtr);

    uint64_t memory_used();

  private:

    void wait_for_previous_buffer();

    void sanity_check_key(KeySpec &key);
    
    ConnectionManagerPtr m_conn_manager_ptr;
    SchemaPtr            m_schema_ptr;
    RangeLocatorPtr      m_range_locator_ptr;
    std::string          m_table_name;
    TableIdentifierT     m_table_identifier;
    uint64_t             m_memory_used;
    uint64_t             m_max_memory;
    MutatorScatterBufferPtr  m_buffer_ptr;
    MutatorScatterBufferPtr  m_prev_buffer_ptr;
  };
  typedef boost::intrusive_ptr<Mutator> MutatorPtr;



}

#endif // HYPERTABLE_MUTATOR_H
