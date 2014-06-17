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

#ifndef HYPERTABLE_DISPATCHHANDLEROPERATIONALTERTABLE_H
#define HYPERTABLE_DISPATCHHANDLEROPERATIONALTERTABLE_H

#include "AsyncComm/CommAddress.h"

#include "DispatchHandlerOperation.h"

namespace Hypertable {

  class DispatchHandlerOperationAlterTable : public DispatchHandlerOperation {
  public:
    DispatchHandlerOperationAlterTable(ContextPtr &context, const TableIdentifier &table, const String &schema) :
      DispatchHandlerOperation(context), m_table(table), m_schema(schema) { }
    virtual void start(const String &location) {
      CommAddress addr;
      addr.set_proxy(location);
      m_rsclient.update_schema(addr, m_table, m_schema, this);
    }
  private:
    TableIdentifierManaged m_table;
    String m_schema;
  };
  typedef intrusive_ptr<DispatchHandlerOperationAlterTable> DispatchHandlerOperationAlterTablePtr;

}

#endif // HYPERTABLE_DISPATCHHANDLEROPERATIONALTERTABLE_H
