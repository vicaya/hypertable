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

#include "Common/Error.h"

#include "Hql.h"
#include "Client.h"
#include "CommandInterpreter.h"

using namespace Hypertable;
using namespace Hypertable::HQL;

CommandInterpreter::CommandInterpreter(Client *client) : m_client(client) {
  return;
}

void CommandInterpreter::execute_line(std::string &line) {
  int error;
  interpreter_state state;
  interpreter interp(state);
  parse_info<> info;

  state.cf = 0;
  state.ag = 0;

  info = parse(line.c_str(), interp, space_p);

  if (info.full) {
    Schema *schema = new Schema();
    for (Schema::AccessGroupMapT::const_iterator ag_iter = state.ag_map.begin(); ag_iter != state.ag_map.end(); ag_iter++)
      schema->add_access_group((*ag_iter).second);
    for (Schema::ColumnFamilyMapT::const_iterator cf_iter = state.cf_map.begin(); cf_iter != state.cf_map.end(); cf_iter++)
      schema->add_column_family((*cf_iter).second);
    const char *error_str = schema->get_error_string();
    if (error_str)
      throw Exception(Error::HQL_PARSE_ERROR, error_str);
    std::string schema_str;
    schema->render(schema_str);

    if ((error = m_client->create_table(state.table_name, schema_str.c_str())) != Error::OK)
      throw Exception(error, std::string("Problem creating table '") + state.table_name + "'");
  }
  else
    throw Exception(Error::HQL_PARSE_ERROR, std::string("parse error at: ") + info.stop);

}
