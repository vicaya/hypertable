/** -*- C++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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
#include "Common/System.h"
#include <fstream>
#include "Config.h"
#include "ReactorFactory.h"

namespace Hypertable { namespace Config {

void init_comm_options() {
  cmdline_desc().add_options()
    ("workers", i32(), "Number of worker threads")
    ("reactors", i32(), "Number of reactor threads")
    ("timeout,t", i32()->default_value(20000),
        "System wide timeout in milliseconds")
    ;
  alias("timeout", "Hypertable.Request.Timeout");
}

void init_comm() {
  int32_t num_cores = System::get_processor_count();

  if (get_bool("verbose"))
    std::cout <<"CPU cores count="<< num_cores << std::endl;

  int32_t reactors = get("reactors", num_cores);

  if (!has("reactors"))
    properties->add("reactors", reactors);

  ReactorFactory::initialize(reactors);
}

void init_generic_server_options() {
  cmdline_desc().add_options()
    ("port", i16(), "Listening port")
    ("pidfile", str(), "File to contain the process id")
    ;
}

void init_generic_server() {
  String pidfile = get("pidfile", String());

  if (pidfile.length()) {
    std::ofstream out(pidfile.c_str());

    if (out)
      out << System::get_pid() << std::endl;
    else
      HT_FATAL_OUT <<"Could not create pid file: "<< pidfile << HT_END;
  }
}

}} // namespace Hypertable::Config
