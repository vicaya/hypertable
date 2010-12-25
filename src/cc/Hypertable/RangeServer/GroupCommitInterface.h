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

#ifndef HYPERSPACE_GROUPCOMMITINTERFACE_H
#define HYPERSPACE_GROUPCOMMITINTERFACE_H

#include <vector>

#include "Common/ReferenceCount.h"
#include "Common/StaticBuffer.h"

#include "AsyncComm/Event.h"

#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Types.h"

#include "Range.h"
#include "TableInfo.h"

namespace __gnu_cxx {
  template<> struct hash<Hypertable::Range *>  {
    size_t operator()(const Hypertable::Range *x) const {
      return (size_t)x;
    }
  };
}

namespace Hypertable {

  struct SendBackRec {
    int error;
    uint32_t count;
    uint32_t offset;
    uint32_t len;
  };

  class UpdateRequest {
  public:
    UpdateRequest() : count(0), error(0) { }
    StaticBuffer buffer;
    uint32_t count;
    EventPtr event;
    std::vector<SendBackRec> send_back_vector;
    uint32_t error;
  };

  class RangeUpdate {
  public:
    DynamicBuffer *bufp;
    uint64_t offset;
    uint64_t len;
  };

  class RangeUpdateList {
  public:
    RangeUpdateList() : starting_update_count(0), last_request(0), split_buf_reset_ptr(0),
                        latest_split_revision(TIMESTAMP_MIN), range_blocked(false) { }
    void reset_updates(UpdateRequest *request) {
      if (request == last_request) {
        if (starting_update_count < updates.size())
          updates.resize(starting_update_count);
        if (split_buf_reset_ptr)
          split_buf.ptr = split_buf_reset_ptr;
      }
    }
    void add_update(UpdateRequest *request, RangeUpdate &update) {
      if (request != last_request) {
        starting_update_count = updates.size();
        last_request = request;
        split_buf_reset_ptr = split_buf.empty() ? 0 : split_buf.ptr;
      }
      if (update.len)
        updates.push_back(update);
    }
    RangePtr range;
    std::vector<RangeUpdate> updates;
    size_t starting_update_count;
    UpdateRequest *last_request;
    DynamicBuffer split_buf;
    uint8_t *split_buf_reset_ptr;
    int64_t latest_split_revision;
    CommitLogPtr splitlog;
    bool range_blocked;
  };

  class TableUpdate {
  public:
    TableUpdate() : flags(0), commit_interval(0), total_count(0),
                    total_buffer_size(0), wait_for_metadata_recovery(false),
                    wait_for_system_recovery(false),
                    split_added(0), total_added(0), error(0) {}
    TableIdentifier id;
    std::vector<UpdateRequest *> requests;
    uint32_t flags;
    uint32_t commit_interval;
    uint32_t commit_iteration;
    uint64_t total_count;
    uint64_t total_buffer_size;
    TableInfoPtr table_info;
    hash_map<Range *, RangeUpdateList *> range_map;
    std::set<Range *> wait_ranges;
    DynamicBuffer go_buf;
    bool wait_for_metadata_recovery;
    bool wait_for_system_recovery;
    uint32_t split_added;
    uint32_t total_added;
    int error;
    String error_msg;
  };

  class GroupCommitInterface : public ReferenceCount {
  public:
    virtual void add(EventPtr &event, SchemaPtr &schema, const TableIdentifier *table,
                     uint32_t count, StaticBuffer &buffer, uint32_t flags) = 0;
    virtual void trigger() = 0;
  };
  typedef boost::intrusive_ptr<GroupCommitInterface> GroupCommitInterfacePtr;
}

namespace __gnu_cxx {
  template<> struct hash<Hypertable::TableIdentifier>  {
    size_t operator()(Hypertable::TableIdentifier tid) const {
      hash<const char*> H;
      return (size_t)H(tid.id) ^ tid.generation;
    }
  };
}

namespace Hypertable {
  struct eqtid {
    bool operator()(TableIdentifier tid1, TableIdentifier tid2) const {
      return strcmp(tid1.id, tid2.id) == 0 && tid1.generation == tid2.generation;
    }
  };
}


#endif // HYPERSPACE_GROUPCOMMITINTERFACE_H

