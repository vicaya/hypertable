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

#include <iostream>

#include <boost/program_options.hpp>
namespace po = boost::program_options;

#include "Common/System.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/HqlCommandInterpreter.h"
#include "Hypertable/Lib/CommandShell.h"

using namespace Hypertable;
using namespace std;

namespace {
  const char *usage_str = 
  "\n" \
  "usage: hypertable [OPTIONS]\n" \
  "\n" \
  "OPTIONS";
}

int main(int argc, char **argv) {
  CommandShellPtr command_shell_ptr;
  CommandInterpreterPtr interp_ptr;
  string configFile = "";
  Client *hypertable;

  try {

    po::options_description desc(usage_str);
    desc.add_options()
      ("help", "Display this help message")
      ;

    CommandShell::add_options(desc);

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
      cout << desc << "\n";
      return 1;
    }

    if (vm.count("config"))
      configFile = vm["config"].as<string>();
    else
      configFile = System::installDir + "/conf/hypertable.cfg";

    hypertable = new Client(argv[0], configFile);

    interp_ptr = hypertable->create_hql_interpreter();

    command_shell_ptr = new CommandShell("hypertable", interp_ptr, vm);

    return command_shell_ptr->run();

  }
  catch(exception& e) {
    cerr << "error: " << e.what() << "\n";
  }
  catch(...) {
    cerr << "Exception of unknown type!\n";
  }

  return 1;
}
