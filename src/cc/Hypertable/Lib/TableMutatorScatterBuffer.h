/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#ifndef HYPERTABLE_TABLEMUTATORSCATTERBUFFER_H
#define HYPERTABLE_TABLEMUTATORSCATTERBUFFER_H

#include <vector>

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>

#include "AsyncComm/DispatchHandler.h"
#include "AsyncComm/Event.h"

#include "Common/atomic.h"
#include "Common/ByteString.h"
#include "Common/FlyweightString.h"
#include "Common/ReferenceCount.h"
#include "Common/StringExt.h"
#include "Common/Timer.h"

#include "Cell.h"
#include "Key.h"
#include "RangeLocator.h"
#include "RangeServerClient.h"
#include "Schema.h"
#include "TableMutatorSendBuffer.h"

namespace Hypertable {

  class TableMutatorScatterBuffer : public ReferenceCount {

  public:

    TableMutatorScatterBuffer(PropertiesPtr &props_ptr, Comm *comm, TableIdentifier *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr);
    void set(Key &key, const void *value, uint32_t value_len, Timer &timer);
    void set_delete(Key &key, Timer &timer);
    void set(ByteString key, ByteString value, Timer &timer);
    bool full() { return m_full; }
    void send();
    bool completed();
    bool wait_for_completion(Timer &timer);
    void reset();
    TableMutatorScatterBuffer *create_redo_buffer(Timer &timer);
    uint64_t get_resend_count() { return m_resends; }
    void get_failed_mutations(std::vector<std::pair<Cell, int> > &failed_mutations) {
      failed_mutations = m_failed_mutations;
    }

  private:

    friend class TableMutatorDispatchHandler;

    typedef hash_map<String, TableMutatorSendBufferPtr> TableMutatorSendBufferMap;

    PropertiesPtr        m_props_ptr;
    Comm                *m_comm;
    SchemaPtr            m_schema_ptr;
    RangeLocatorPtr      m_range_locator_ptr;
    LocationCachePtr     m_cache_ptr;
    RangeServerClient    m_range_server;
    TableIdentifierManaged m_table_identifier;
    TableMutatorSendBufferMap m_buffer_map;
    TableMutatorCompletionCounter m_completion_counter;
    bool                 m_full;
    uint64_t             m_resends;
    std::vector<std::pair<Cell, int> > m_failed_mutations;
    FlyweightString      m_constant_strings;

  };
  typedef boost::intrusive_ptr<TableMutatorScatterBuffer> TableMutatorScatterBufferPtr;


}

#endif // HYPERTABLE_TABLEMUTATORSCATTERBUFFER_H
