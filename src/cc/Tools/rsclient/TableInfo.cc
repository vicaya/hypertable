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

#include "Common/Compat.h"
#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"

#include "Hyperspace/Session.h"

#include "TableInfo.h"

namespace Hypertable {
  using namespace Hyperspace;

  TableInfo::TableInfo(const String &table_name) {
    memset(&m_table, 0, sizeof(TableIdentifier));
    m_table.name = new char [strlen(table_name.c_str()) + 1];
    strcpy((char *)m_table.name, table_name.c_str());
  }

  void TableInfo::load(Hyperspace::SessionPtr &hyperspace_ptr) {
    String table_file = (String)"/hypertable/tables/" + m_table.name;
    DynamicBuffer valbuf(0);
    HandleCallbackPtr null_handle_callback;
    uint64_t handle;

    handle = hyperspace_ptr->open(table_file.c_str(), OPEN_FLAG_READ,
                                  null_handle_callback);

    hyperspace_ptr->attr_get(handle, "schema", valbuf);

    Schema *schema = Schema::new_instance((const char *)valbuf.base,
                                          valbuf.fill(), true);
    if (!schema->is_valid())
      HT_THROWF(Error::RANGESERVER_SCHEMA_PARSE_ERROR,
                "Schema Parse Error: %s", schema->get_error_string());
    m_schema_ptr = schema;

    m_table.generation = schema->get_generation();

    valbuf.clear();
    hyperspace_ptr->attr_get(handle, "table_id", valbuf);

    if (valbuf.fill() != sizeof(int32_t))
      HT_THROWF(Error::INVALID_METADATA, "%s/table_id contains a bad value",
                table_file.c_str());

    memcpy(&m_table.id, valbuf.base, sizeof(int32_t));

    hyperspace_ptr->close(handle);

  }
}
