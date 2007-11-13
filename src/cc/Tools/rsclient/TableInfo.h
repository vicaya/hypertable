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

#ifndef HYPERTABLE_RSCLIENT_TABLEINFO_H
#define HYPERTABLE_RSCLIENT_TABLEINFO_H

#include <string>

#include <ext/hash_map>

#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Types.h"

#include "Hyperspace/Session.h"

namespace Hypertable {

  class TableInfo {
  public:
    TableInfo(std::string &table_name);

    int load(Hyperspace::SessionPtr &hyperspace_ptr);

    TableIdentifierT *get_table_identifier() { return &m_table; }
    void get_schema_ptr(SchemaPtr &schema_ptr) { schema_ptr = m_schema_ptr; }

    typedef __gnu_cxx::hash_map<std::string, TableInfo *> MapT;

    static MapT map;

  private:
    TableIdentifierT  m_table;
    SchemaPtr         m_schema_ptr;
  };
}

#endif // HYPERTABLE_RSCLIENT_TABLEINFO_H
