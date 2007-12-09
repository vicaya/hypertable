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

#ifndef HYPERTABLE_INTERACTIVECOMMAND_H
#define HYPERTABLE_INTERACTIVECOMMAND_H

#include <cstring>

#include <utility>
#include <vector>

namespace Hypertable {

  class InteractiveCommand {

  public:

    virtual ~InteractiveCommand() { return; }

    void parse_command_line(const char *line);
    bool matches(const char *line) { return !strncmp(line, command_text(), strlen(command_text())); }

    virtual const char *command_text() = 0;
    virtual const char **usage() = 0;
    virtual int run() = 0;

    void clear_args() { m_args.clear(); }
    void push_arg(std::string key, std::string value) { 
      m_args.push_back( std::pair<std::string, std::string>(key, value) );
    }

  protected:
    std::vector< std::pair<std::string, std::string> >  m_args;

    bool parse_string_literal(const char *str, std::string &text, const char **endptr);
  };

}

#endif // HYPERTABLE_INTERACTIVECOMMAND_H
