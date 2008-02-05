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

#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"

#include "Hyperspace/Session.h"

#include "TableInfo.h"

namespace Hypertable {

  TableInfo::MapT  TableInfo::map;

  TableInfo::TableInfo(std::string &table_name) {
    memset(&m_table, 0, sizeof(TableIdentifierT));
    m_table.name = new char [ strlen(table_name.c_str()) + 1 ];
    strcpy((char *)m_table.name, table_name.c_str());
  }

  int TableInfo::load(Hyperspace::SessionPtr &hyperspace_ptr) {
    std::string table_file = (std::string)"/hypertable/tables/" + m_table.name;
    DynamicBuffer valueBuf(0);
    int error;
    HandleCallbackPtr nullHandleCallback;
    uint64_t handle;

    if ((error = hyperspace_ptr->open(table_file.c_str(), OPEN_FLAG_READ, nullHandleCallback, &handle)) != Error::OK) {
      HT_ERRORF("Unable to open Hyperspace file '%s' (%s)", table_file.c_str(), Error::get_text(error));
      return error;
    }

    if ((error = hyperspace_ptr->attr_get(handle, "schema", valueBuf)) != Error::OK) {
      HT_ERRORF("Problem getting 'schema' attribute for '%s' - %s", m_table.name, Error::get_text(error));
      return error;
    }

    Schema *schema = Schema::new_instance((const char *)valueBuf.buf, valueBuf.fill(), true);
    if (!schema->is_valid()) {
      HT_ERRORF("Schema Parse Error: %s", schema->get_error_string());
      delete schema;
      return Error::RANGESERVER_SCHEMA_PARSE_ERROR;
    }
    m_schema_ptr = schema;

    m_table.generation = schema->get_generation();

    valueBuf.clear();
    if ((error = hyperspace_ptr->attr_get(handle, "table_id", valueBuf)) != Error::OK) {
      HT_ERRORF("Problem getting attribute 'table_id' from '%s' - %s", m_table.name, Error::get_text(error));
      return error;
    }

    if (valueBuf.fill() != sizeof(int32_t)) {
      HT_ERRORF("%s/table_id contains a bad value", table_file.c_str());
      return Error::INVALID_METADATA;
    }

    memcpy(&m_table.id, valueBuf.buf, sizeof(int32_t));

    hyperspace_ptr->close(handle);

    return Error::OK;
  }
}
