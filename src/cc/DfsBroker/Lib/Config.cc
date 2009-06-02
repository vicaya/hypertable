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
#include "Common/InetAddr.h"
#include "Config.h"

namespace Hypertable { namespace Config {

void init_dfs_client_options() {
  cmdline_desc().add_options()
    ("dfs", str()->default_value("localhost:38030"),
        "DFS client endpoint in <host:port> format")
    ("dfs-timeout", i32(),
        "Timeout in milliseconds for DFS client connections")
    ;
  alias("dfs-timeout", "DfsBroker.Timeout");
  // hidden aliases
  alias("dfs-host", "DfsBroker.Host");
  alias("dfs-port", "DfsBroker.Port");
}

void init_dfs_client() {
  // prepare hidden aliases to be synced
  Endpoint e = InetAddr::parse_endpoint(get_str("dfs"));
  bool defaulted = properties->defaulted("dfs");
  properties->set("dfs-host", e.host, defaulted);
  properties->set("dfs-port", e.port, !e.port || defaulted);
}

void init_dfs_broker_options() {
  cmdline_desc().add_options()
    ("port", i16()->default_value(38030), "Listening port")
    ("pidfile", str(), "File to contain the process id")
    ;
}

}} // namespace Hypertable::Config
