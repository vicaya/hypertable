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

#include "Common/Compat.h"

#include "Hypertable/Lib/HqlHelpText.h"
#include "Hypertable/Lib/HqlParser.h"

#include "MasterCommandInterpreter.h"

using namespace Hypertable;
using namespace Hql;
using namespace std;

MasterCommandInterpreter::MasterCommandInterpreter(Comm *comm,
           const sockaddr_in addr, MasterClientPtr &master)
  : m_comm(comm), m_addr(addr), m_master(master) {
  HqlHelpText::install_master_client_text();
}


void MasterCommandInterpreter::execute_line(const String &line) {
  Hql::ParserState state;
  Hql::Parser parser(state);
  parse_info<> info;

  info = parse(line.c_str(), parser, space_p);

  if (info.full) {

    if (state.command == COMMAND_SHUTDOWN) {
      m_master->shutdown();
    }
    else if (state.command == COMMAND_HELP) {
      const char **text = HqlHelpText::get(state.str);
      if (text) {
        for (size_t i=0; text[i]; i++)
          cout << text[i] << endl;
      }
      else
        cout << endl << "no help for '" << state.str << "'" << endl << endl;
    }
    else
      HT_THROW(Error::HQL_PARSE_ERROR, "unsupported command");
  }
  else
    HT_THROW(Error::HQL_PARSE_ERROR, String("parse error at: ") + info.stop);
}

