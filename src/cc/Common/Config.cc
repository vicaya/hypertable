/**
 * Copyright (C) 2007 Luke Lu (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License.
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
#include "Common/Version.h"
#include "Common/Logger.h"
#include "Common/String.h"
#include <iostream>
#include "Config.h"

namespace Hypertable { namespace Config {

// singletons
VarMap varmap;
String cfgfile;

Desc *descp = NULL;

// functions
Desc &description(const char *name) {
  if (!descp)
    descp = new Desc(format("Usage: %s [options]\nOptions",
                            name ? name : System::exe_name.c_str()));
  return *descp;
}

void description(const Desc &desc) {
  if (descp)
    delete descp;
  descp = new Desc(desc);
}

void init_default_options() {
  description().add_options()
    ("help,h", "Show this help message and exit")
    ("version", "Show version information and exit")
    ("verbose,v", "Show more verbose (debug) output")
    ("log-level,l", value<String>()->default_value("info"),
        "Logging level: debug, info, or error")
    ("config,c", value<String>(), "Configuration file.\n"
        "default: <install_dir>/conf/hypertable.cfg")
    ;
}

void parse_args(int argc, char *argv[], const Desc *desc, const Desc *hidden,
                const PositionalDesc *p) {
  const Desc &pub_desc = description();

  try {
    command_line_parser parser(argc, argv);
    Desc full(pub_desc);

    if (hidden)
      full.add(*hidden);

    parser.options(full);

    if (p)
      parser.positional(*p);

    store(parser.run(), varmap);
    notify(varmap);
  }
  catch (std::exception &e) {
    HT_ERROR_OUT << e.what() << HT_END;
    std::cout << pub_desc << std::endl;
    exit(1);
  }

  // some builtin behavior
  if (varmap.count("help")) {
    std::cout << pub_desc << std::endl;
    std::exit(1);
  }

  if (varmap.count("version")) {
    std::cout << version() << std::endl;
    std::exit(1);
  }

  String loglevel = varmap["log-level"].as<String>();

  if (loglevel == "info")
    Logger::set_level(Logger::Priority::INFO);
  else if (loglevel == "debug")
    Logger::set_level(Logger::Priority::DEBUG);
  else if (loglevel == "error")
    Logger::set_level(Logger::Priority::ERROR);
  else {
    HT_ERROR_OUT << "unknown log level: "<< loglevel << HT_END;
    std::exit(1);
  }

  cfgfile = varmap.count("config") ? varmap["config"].as<String>() :
      format("%s/conf/hypertable.cfg", System::install_dir.c_str());
}

void init_default_actions() {
  // TODO: load config file etc?
}

}} // namespace Hypertable::Config
