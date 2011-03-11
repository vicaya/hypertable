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

#ifndef HYPERTABLE_OPERATIONRENAMETABLE_H
#define HYPERTABLE_OPERATIONRENAMETABLE_H

#include "Operation.h"

namespace Hypertable {

  class OperationRenameTable : public Operation {
  public:
    OperationRenameTable(ContextPtr &context, const String &old_name, const String &new_name);
    OperationRenameTable(ContextPtr &context, const MetaLog::EntityHeader &header_);
    OperationRenameTable(ContextPtr &context, EventPtr &event);
    virtual ~OperationRenameTable() { }

    virtual void execute();
    virtual const String name();
    virtual const String label();
    virtual void display_state(std::ostream &os);
    virtual size_t encoded_state_length() const;
    virtual void encode_state(uint8_t **bufp) const;
    virtual void decode_state(const uint8_t **bufp, size_t *remainp);
    virtual void decode_request(const uint8_t **bufp, size_t *remainp);

  private:
    void initialize_dependencies();
    String m_old_name;
    String m_new_name;
    String m_id;
  };


  typedef intrusive_ptr<OperationRenameTable> OperationRenameTablePtr;

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONRENAMETABLE_H
