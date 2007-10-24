/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_TABLE_H
#define HYPERTABLE_TABLE_H

#include "Common/ReferenceCount.h"

#include "Mutator.h"
#include "Schema.h"

namespace hypertable {

  class ConnectionManager;

  class Table : public ReferenceCount {

  public:
    Table(ConnectionManager *connManager, std::string &name);
    virtual ~Table() { return; }

    int CreateMutator(MutatorPtr &mutatorPtr);

  private:
    ConnectionManager *mConnManager;
    SchemaPtr          mSchemaPtr;
  };
  typedef boost::intrusive_ptr<Table> TablePtr;

}

#endif // HYPERTABLE_TABLE_H
