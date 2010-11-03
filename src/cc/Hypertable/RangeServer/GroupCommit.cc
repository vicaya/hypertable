/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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
#include "Common/Compat.h"
#include "Common/Config.h"

#include "GroupCommit.h"
#include "RangeServer.h"


using namespace Hypertable;
using namespace Hypertable::Config;

GroupCommit::GroupCommit(RangeServer *range_server) : m_range_server(range_server), m_counter(0) {

  m_commit_interval = get_i32("Hypertable.RangeServer.CommitInterval");
  
}


void GroupCommit::add(EventPtr &event, SchemaPtr &schema, const TableIdentifier *table,
                      uint32_t count, StaticBuffer &buffer, uint32_t flags) {
  ScopedLock lock(m_mutex);
  TableUpdateMap::iterator iter;
  UpdateRequest *request = new UpdateRequest();

  request->buffer = buffer;
  request->count = count;
  request->event = event;

  if ((iter = m_table_map.find(*table)) == m_table_map.end()) {
    TableIdentifier tid;

    tid.generation = table->generation;
    tid.id = m_flyweight_strings.get(table->id);

    TableUpdate *tu = new TableUpdate();
    memcpy(&tu->id, &tid, sizeof(TableIdentifier));
    tu->commit_interval = schema->get_group_commit_interval();
    tu->commit_iteration = (tu->commit_interval+(m_commit_interval-1)) / m_commit_interval;
    tu->total_count = count;
    tu->total_buffer_size = buffer.size;
    tu->requests.push_back(request);
    
    m_table_map[tid] = tu;
    return;
  }

  (*iter).second->total_count += count;
  (*iter).second->total_buffer_size += buffer.size;
  (*iter).second->requests.push_back(request);
}



void GroupCommit::trigger() {
  ScopedLock lock(m_mutex);
  std::vector<TableUpdate *> updates;

  m_counter++;

  TableUpdateMap::iterator iter = m_table_map.begin();
  while (iter != m_table_map.end()) {
    if ((m_counter % (*iter).second->commit_iteration) == 0) {
      TableUpdateMap::iterator remove_iter = iter;
      ++iter;
      updates.push_back((*remove_iter).second);
      m_table_map.erase(remove_iter);
    }
    else
      ++iter;
  }

  if (!updates.empty()) {
    m_range_server->batch_update(updates);

    // Free objects
    foreach (TableUpdate *table_update, updates) {
      foreach (UpdateRequest *request, table_update->requests)
        delete request;
      delete table_update;
    }
  }

}
