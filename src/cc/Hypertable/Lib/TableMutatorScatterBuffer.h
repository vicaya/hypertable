/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include "Common/InetAddr.h"

#include "Cell.h"
#include "Key.h"
#include "RangeLocator.h"
#include "Schema.h"
#include "TableMutatorSendBuffer.h"

namespace Hypertable {

  typedef std::pair<Cell, int> FailedMutation;
  typedef std::vector<FailedMutation> FailedMutations;
  typedef hash_map<String, uint32_t> RangeServerFlagsMap;

  class TableMutatorScatterBuffer : public ReferenceCount {

  public:
    TableMutatorScatterBuffer(Comm *, const TableIdentifier *,
                              SchemaPtr &, RangeLocatorPtr &, uint32_t timeout_ms);
    void set(const Key &, const void *value, uint32_t value_len, Timer &timer);
    void set_delete(const Key &key, Timer &timer);
    void set(SerializedKey key, ByteString value, Timer &timer);
    bool full() { return m_full; }
    void send(RangeServerFlagsMap &rangeserver_flags_map, uint32_t flags);
    bool completed();
    bool wait_for_completion(Timer &timer);
    void reset();
    TableMutatorScatterBuffer *create_redo_buffer(Timer &timer);
    uint64_t get_resend_count() { return m_resends; }
    void
    get_failed_mutations(FailedMutations &failed_mutations) {
      failed_mutations = m_failed_mutations;
    }
    size_t get_failure_count() { return m_failed_mutations.size(); }
    void refresh_schema(const TableIdentifier &table_id, SchemaPtr &schema) {
      m_schema = schema;
      m_table_identifier = table_id;
    }
  private:

    friend class TableMutatorDispatchHandler;

    typedef hash_map<String, TableMutatorSendBufferPtr>
            TableMutatorSendBufferMap;

    Comm                *m_comm;
    SchemaPtr            m_schema;
    RangeLocatorPtr      m_range_locator;
    LocationCachePtr     m_loc_cache;
    RangeServerClient    m_range_server;
    TableIdentifierManaged m_table_identifier;
    TableMutatorSendBufferMap m_buffer_map;
    TableMutatorCompletionCounter m_completion_counter;
    bool                 m_full;
    uint64_t             m_resends;
    FailedMutations      m_failed_mutations;
    FlyweightString      m_constant_strings;
    uint32_t             m_timeout_ms;
    uint32_t             m_server_flush_limit;
    uint32_t             m_last_send_flags;
    bool                 m_refresh_schema;
  };

  typedef intrusive_ptr<TableMutatorScatterBuffer> TableMutatorScatterBufferPtr;

} // namespace Hypertable

#endif // HYPERTABLE_TABLEMUTATORSCATTERBUFFER_H
