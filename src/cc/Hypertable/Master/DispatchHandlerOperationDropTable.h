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

#ifndef HYPERTABLE_DISPATCHHANDLEROPERATIONDROPTABLE_H
#define HYPERTABLE_DISPATCHHANDLEROPERATIONDROPTABLE_H

#include "AsyncComm/CommAddress.h"

#include "DispatchHandlerOperation.h"

namespace Hypertable {

  class DispatchHandlerOperationDropTable : public DispatchHandlerOperation {
  public:
    DispatchHandlerOperationDropTable(ContextPtr &context, const TableIdentifier &table) :
      DispatchHandlerOperation(context), m_table(table) { }
    virtual void start(const String &location) {
      CommAddress addr;
      addr.set_proxy(location);
      m_rsclient.drop_table(addr, m_table, this);
    }
  private:
    TableIdentifierManaged m_table;
  };
  typedef intrusive_ptr<DispatchHandlerOperationDropTable> DispatchHandlerOperationDropTablePtr;

}

#endif // HYPERTABLE_DISPATCHHANDLEROPERATIONDROPTABLE_H
