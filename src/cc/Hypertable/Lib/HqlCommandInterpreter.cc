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

#include <cstring>

#include "Common/Error.h"

#include "Client.h"
#include "HqlCommandInterpreter.h"
#include "HqlHelpText.h"
#include "HqlParser.h"

using namespace Hypertable;
using namespace Hypertable::HqlHelpText;
using namespace Hypertable::HqlParser;

HqlCommandInterpreter::HqlCommandInterpreter(Client *client) : m_client(client) {
  return;
}

void HqlCommandInterpreter::execute_line(std::string &line) {
  int error;
  std::string schema_str;
  std::string out_str;
  hql_interpreter_state state;
  hql_interpreter interp(state);
  parse_info<> info;
  Schema *schema;

  info = parse(line.c_str(), interp, space_p);

  if (info.full) {

    if (state.command == COMMAND_SHOW_CREATE_TABLE) {
      if ((error = m_client->get_schema(state.table_name, schema_str)) != Error::OK)
	throw Exception(error, std::string("Problem fetching schema for table '") + state.table_name + "' from master");
      schema = Schema::new_instance(schema_str.c_str(), strlen(schema_str.c_str()), true);
      if (!schema->is_valid())
	throw Exception(Error::BAD_SCHEMA, schema->get_error_string());
      schema->render_hql_create_table(state.table_name, out_str);
      cout << out_str << flush;
    }
    else if (state.command == COMMAND_HELP) {
      if (state.str == "contents")
	cout << help_text_summary << flush;
      else
	cout << endl << "no help for '" << state.str << "'" << endl << endl;
    }
    else if (state.command == COMMAND_CREATE_TABLE) {
      schema = new Schema();
      for (Schema::AccessGroupMapT::const_iterator ag_iter = state.ag_map.begin(); ag_iter != state.ag_map.end(); ag_iter++)
	schema->add_access_group((*ag_iter).second);
      for (Schema::ColumnFamilyMapT::const_iterator cf_iter = state.cf_map.begin(); cf_iter != state.cf_map.end(); cf_iter++)
	schema->add_column_family((*cf_iter).second);
      const char *error_str = schema->get_error_string();
      if (error_str)
	throw Exception(Error::HQL_PARSE_ERROR, error_str);
      schema->render(schema_str);

      if ((error = m_client->create_table(state.table_name, schema_str.c_str())) != Error::OK)
	throw Exception(error, std::string("Problem creating table '") + state.table_name + "'");
    }
    else if (state.command == COMMAND_DESCRIBE_TABLE) {
      if ((error = m_client->get_schema(state.table_name, schema_str)) != Error::OK)
	throw Exception(error, std::string("Problem fetching schema for table '") + state.table_name + "' from master");
      cout << schema_str << endl;
    }
    else if (state.command == COMMAND_SELECT) {
      time_t t;
      cout << "column families = ";
      for (size_t i=0; i<state.scan.columns.size(); i++)
	cout << state.scan.columns[i] << " ";
      cout << endl;
      cout << "row = " << state.scan.row << endl;
      cout << "start_row = " << state.scan.start_row << endl;
      cout << "start_row_inclusive = " << state.scan.start_row_inclusive << endl;
      cout << "end_row = " << state.scan.end_row << endl;
      cout << "end_row_inclusive = " << state.scan.end_row_inclusive << endl;
      cout << "max_versions = " << state.scan.max_versions << endl;
      cout << "limit = " << state.scan.limit << endl;
      t = state.scan.start_time / 1000000LL;
      cout << "start_time = " << ctime(&t) << endl;
      t = state.scan.end_time / 1000000LL;
      cout << "end_time = " << ctime(&t) << endl;
    }
  }
  else
    throw Exception(Error::HQL_PARSE_ERROR, std::string("parse error at: ") + info.stop);

}
