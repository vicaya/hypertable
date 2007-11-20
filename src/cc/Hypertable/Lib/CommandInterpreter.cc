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

#include "CommandInterpreter.h"

using namespace Hypertable;
using namespace Hypertable::HQL;

CommandInterpreter::CommandInterpreter(Client *client) : m_client(client) {
  return;
}

void CommandInterpreter::execute_line(std::string &line) {
  interpreter_state state;
  interpreter interp(state);

  state.cf = 0;
  state.ag = 0;

  parse_info<> info = parse(line.c_str(), interp, space_p);

  if (info.full) {
    cout << "-------------------------\n";
    cout << "Parsing succeeded\n";
    cout << "Command: " << state.command << endl;
    cout << "Table: " << state.table_name << endl;
    {
      for (Schema::ColumnFamilyMapT::const_iterator iter = state.cf_map.begin(); iter != state.cf_map.end(); iter++) {
	cout << "  Column Family = " << (*iter).first << endl;
	cout << "    ttl = " << (*iter).second->expireTime << endl;
	cout << "    max = " << (*iter).second->cellLimit << endl;
      }
    }
    cout << "-------------------------\n";
  }
  else
    throw Exception(Error::HQL_PARSE_ERROR, std::string("parse error at: ") + info.stop);
  
}
