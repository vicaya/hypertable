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

#ifndef HYPERTABLE_TABLEMUTATORSENDBUFFER_H
#define HYPERTABLE_TABLEMUTATORSENDBUFFER_H

#include "Common/ReferenceCount.h"

#include "TableMutatorCompletionCounter.h"

namespace Hypertable {

  typedef struct {
    int error;
    uint8_t *base;
    uint32_t len;
  } FailedRegionT;

  /**
   * 
   */
  class TableMutatorSendBuffer : public ReferenceCount {
  public:
    TableMutatorSendBuffer(TableIdentifier *tid, TableMutatorCompletionCounter *cc, RangeLocator *rl) : counterp(cc), m_table_identifier(tid), m_range_locator(rl) { return; }
    void add_retries(uint32_t offset, uint32_t len) {
      accum.add(pending_updates.base+offset, len);
      counterp->set_retries();
      pending_updates.own = true;
      // invalidate row key
      const uint8_t *ptr = pending_updates.base+offset;
      Serialization::decode_vi32(&ptr);
      m_range_locator->invalidate(m_table_identifier, (const char *)ptr);
    }
    void add_retries_all() {
      accum.add(pending_updates.base, pending_updates.size);
      counterp->set_retries();
      pending_updates.own = true;
      // invalidate row key
      const uint8_t *ptr = pending_updates.base;
      Serialization::decode_vi32(&ptr);
      m_range_locator->invalidate(m_table_identifier, (const char *)ptr);
    }
    void add_errors(int error, uint32_t offset, uint32_t len) {
      FailedRegionT failed;
      failed.error = error;
      failed.base = pending_updates.base + offset;
      failed.len = len;
      failed_regions.push_back(failed);
      counterp->set_errors();
      pending_updates.own = true;
    }
    void add_errors_all(uint32_t error) {
      FailedRegionT failed;
      failed.error = (int)error;
      failed.base = pending_updates.base;
      failed.len = pending_updates.size;
      failed_regions.push_back(failed);
      counterp->set_errors();
      pending_updates.own = true;
    }
    void clear() {
      key_offsets.clear();
      accum.clear();
      delete [] pending_updates.base;
      pending_updates.base = 0;
      pending_updates.size = 0;
      pending_updates.own = true;
      failed_regions.clear();
    }
    void reset() {
      clear();
      dispatch_handler_ptr = 0;
    }
    void get_failed_regions(std::vector<FailedRegionT> &errors) {
      errors.insert(errors.end(), failed_regions.begin(), failed_regions.end());
    }

    std::vector<uint64_t> key_offsets;
    DynamicBuffer accum;
    StaticBuffer pending_updates;
    struct sockaddr_in addr;
    TableMutatorCompletionCounter *counterp;
    DispatchHandlerPtr dispatch_handler_ptr;
    std::vector<FailedRegionT> failed_regions;
  private:
    TableIdentifier *m_table_identifier;
    RangeLocator *m_range_locator;
  };
  typedef boost::intrusive_ptr<TableMutatorSendBuffer> TableMutatorSendBufferPtr;

}

#endif // HYPERTABLE_TABLEMUTATORSENDBUFFER_H
