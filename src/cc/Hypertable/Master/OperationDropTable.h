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

#ifndef HYPERTABLE_OPERATIONDROPTABLE_H
#define HYPERTABLE_OPERATIONDROPTABLE_H

#include "Common/StringExt.h"

#include "Operation.h"

namespace Hypertable {

  class OperationDropTable : public Operation {
  public:
    OperationDropTable(ContextPtr &context, const String &name, bool if_exists);
    OperationDropTable(ContextPtr &context, const MetaLog::EntityHeader &header_);
    OperationDropTable(ContextPtr &context, EventPtr &event);
    virtual ~OperationDropTable() { }

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
    String m_name;
    bool m_if_exists;
    String m_id;
    StringSet m_completed;
  };

  typedef intrusive_ptr<OperationDropTable> OperationDropTablePtr;

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONDROPTABLE_H
