/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
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

#ifndef HYPERTABLE_OPERATIONMOVERANGE_H
#define HYPERTABLE_OPERATIONMOVERANGE_H

#include "Operation.h"

namespace Hypertable {

  class OperationMoveRange : public Operation {
  public:
    OperationMoveRange(ContextPtr &context, const TableIdentifier &table,
                       const RangeSpec &range, const String &transfer_log,
                       uint64_t soft_limit, bool is_split);
    OperationMoveRange(ContextPtr &context, const MetaLog::EntityHeader &header_);
    OperationMoveRange(ContextPtr &context, EventPtr &event);
    virtual ~OperationMoveRange() { }

    void initialize_dependencies();

    virtual void execute();
    virtual const String name();
    virtual const String label();
    virtual const String graphviz_label();
    virtual void display_state(std::ostream &os);
    virtual size_t encoded_state_length() const;
    virtual void encode_state(uint8_t **bufp) const;
    virtual void decode_state(const uint8_t **bufp, size_t *remainp);
    virtual void decode_request(const uint8_t **bufp, size_t *remainp);
    virtual int64_t hash_code() const;
    virtual bool remove_explicitly() { return m_remove_explicitly; }

    String get_location() { return m_location; }
    void abort_operation();

  private:
    TableIdentifierManaged m_table;
    RangeSpecManaged m_range;
    String m_transfer_log;
    uint64_t m_soft_limit;
    bool m_is_split;
    String m_location;
    String m_range_name;
    bool m_remove_explicitly;
  };

  typedef intrusive_ptr<OperationMoveRange> OperationMoveRangePtr;

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONMOVERANGE_H
