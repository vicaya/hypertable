/**
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include "Common/InetAddr.h"
#include "Common/CommandShell.h"
#include "Config.h"

namespace Hypertable { namespace Config {

void init_command_shell_options() {
  CommandShell::add_options(cmdline_desc());
}

void init_master_client_options() {
  cmdline_desc().add_options()
    ("master", str()->default_value("localhost:38050"),
        "master server to connect in <host:port> format")
    ("master-timeout", i32()->default_value(30000),
        "Timeout in seconds for master connection")
    ;
  alias("master-timeout", "Hypertable.Master.Timeout");
  // hidden aliases
  alias("master-host", "Hypertable.Master.Host");
  alias("master-port", "Hypertable.Master.Port");
}

void init_master_client() {
  Endpoint e = InetAddr::parse_endpoint(get_str("master"));
  bool defaulted = properties->defaulted("master");
  properties->set("master-host", e.host, defaulted);
  properties->set("master-port", e.port, !e.port || defaulted);
}

void init_range_server_client_options() {
  cmdline_desc().add_options()
    ("range-server", str()->default_value("localhost:38060"),
        "range server to connect in <host:port> format")
    ("range-server-timeout", i32()->default_value(30000),
        "Timeout in seconds for range server connection")
    ;
  alias("range-server-timeout", "Hypertable.RangeServer.Timeout");
  // hidden aliases
  alias("rs-port", "Hypertable.RangeServer.Port");
}

void init_range_server_client() {
  Endpoint e = InetAddr::parse_endpoint(get_str("range-server"));
  bool defaulted = properties->defaulted("range-server");
  properties->set("rs-host", e.host, defaulted);
  properties->set("rs-port", e.port, !e.port || defaulted);
}

}} // namespace Hypertable::Config
