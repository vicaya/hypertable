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

#ifndef HYPERTABLE_TABLEMUTATORSENDBUFFER_H
#define HYPERTABLE_TABLEMUTATORSENDBUFFER_H

#include "Common/ReferenceCount.h"

#include "TableMutatorCompletionCounter.h"

namespace Hypertable {

  struct FailedRegion {
    int error;
    uint8_t *base;
    uint32_t len;
  };

  /**
   *
   */
  class TableMutatorSendBuffer : public ReferenceCount {
  public:
    TableMutatorSendBuffer(const TableIdentifier *tid,
        TableMutatorCompletionCounter *cc, RangeLocator *rl)
      : counterp(cc), send_count(0), retry_count(0), m_table_identifier(tid),
        m_range_locator(rl) { }

    void add_retries(uint32_t count, uint32_t offset, uint32_t len) {
      accum.add(pending_updates.base+offset, len);
      counterp->set_retries();
      retry_count += count;
      // invalidate row key
      SerializedKey key(pending_updates.base+offset);
      m_range_locator->invalidate(m_table_identifier, key.row());
    }
    void add_retries_all(bool with_error=false, uint32_t error=0) {
      accum.add(pending_updates.base, pending_updates.size);
      counterp->set_retries();
      retry_count = send_count;
      // invalidate row key
      SerializedKey key(pending_updates.base);
      m_range_locator->invalidate(m_table_identifier, key.row());
      if (with_error) {
        FailedRegion failed;
        failed.error=(int) error;
        failed.base = pending_updates.base;
        failed.len = pending_updates.size;
        failed_regions.push_back(failed);
        counterp->set_errors();
      }
    }
    void add_errors(int error, uint32_t count, uint32_t offset, uint32_t len) {
      FailedRegion failed;
      (void)count;
      failed.error = error;
      failed.base = pending_updates.base + offset;
      failed.len = len;
      failed_regions.push_back(failed);
      counterp->set_errors();
    }
    void add_errors_all(uint32_t error) {
      FailedRegion failed;
      failed.error = (int)error;
      failed.base = pending_updates.base;
      failed.len = pending_updates.size;
      failed_regions.push_back(failed);
      counterp->set_errors();
    }
    void clear() {
      key_offsets.clear();
      accum.clear();
      pending_updates.free();
      failed_regions.clear();
      send_count = 0;
      retry_count = 0;
    }
    void reset() {
      clear();
      dispatch_handler = 0;
    }
    void get_failed_regions(std::vector<FailedRegion> &errors) {
      errors.insert(errors.end(), failed_regions.begin(), failed_regions.end());
    }

    bool resend() { return retry_count > 0; }

    std::vector<uint64_t> key_offsets;
    DynamicBuffer accum;
    StaticBuffer pending_updates;
    struct sockaddr_in addr;
    TableMutatorCompletionCounter *counterp;
    DispatchHandlerPtr dispatch_handler;
    std::vector<FailedRegion> failed_regions;
    uint32_t send_count;
    uint32_t retry_count;

  private:
    const TableIdentifier *m_table_identifier;
    RangeLocator *m_range_locator;
  };

  typedef intrusive_ptr<TableMutatorSendBuffer> TableMutatorSendBufferPtr;

} // namespace Hypertable

#endif // HYPERTABLE_TABLEMUTATORSENDBUFFER_H
