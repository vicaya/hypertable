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

#ifndef HYPERTABLE_RSCLIENT_TABLEINFO_H
#define HYPERTABLE_RSCLIENT_TABLEINFO_H

#include "Common/String.h"

#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Types.h"

#include "Hyperspace/Session.h"

namespace Hypertable {

  class TableInfo {
  public:
    TableInfo(const String &toplevel_dir, const String &table_id);

    void load(Hyperspace::SessionPtr &hyperspace);

    TableIdentifier *get_table_identifier() { return &m_table; }
    void get_schema_ptr(SchemaPtr &schema) { schema = m_schema; }

  private:
    TableIdentifierManaged m_table;
    SchemaPtr m_schema;
    String m_toplevel_dir;
  };
}

#endif // HYPERTABLE_RSCLIENT_TABLEINFO_H
