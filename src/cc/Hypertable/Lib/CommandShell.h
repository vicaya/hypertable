/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_COMMAND_SHELL_H
#define HYPERTABLE_COMMAND_SHELL_H

#include <string>

#include <boost/program_options.hpp>
namespace po = boost::program_options;

#include "Common/ReferenceCount.h"

#include "CommandInterpreter.h"

namespace Hypertable {

  class CommandShell : public ReferenceCount {
  public:
    CommandShell(std::string program_name, CommandInterpreterPtr &interp_ptr, po::variables_map &vm);
    int run();

    static void add_options(po::options_description &desc);

    static std::string ms_history_file;

  private:
    char *rl_gets ();

    std::string m_program_name;
    CommandInterpreterPtr m_interp_ptr;
    po::variables_map m_varmap;
    

    std::string g_accum;
    bool g_batch_mode;
    bool g_no_prompt;
    bool g_cont;
    char *line_read;
    std::string gInputStr;
  };
  typedef boost::intrusive_ptr<CommandShell> CommandShellPtr;

}

#endif // HYPERTABLE_COMMAND_SHELL_H

