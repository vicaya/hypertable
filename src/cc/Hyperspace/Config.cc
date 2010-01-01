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

void init_hyperspace_client_options() {
  cmdline_desc().add_options()
    ("hyperspace", str()->default_value("localhost:38040"),
        "hyperspace master server to connect to in <host:port> format")
    ("hyperspace-timeout", i32()->default_value(30000),
        "Timeout in milliseconds for hyperspace connection")
    ("keepalive", i32()->default_value(10000),
        "Interval in milliseconds of keepalive message from Hyperspace client")
    ("lease-interval", i32()->default_value(60000),
        "Hyperspace master lease interval in milliseconds")
    ("grace-period", i32()->default_value(60000),
        "Grace period in milliseconds before 'jeopardy' for hyperspace client")
    ;
  alias("hyperspace-timeout", "Hyperspace.Timeout");
  alias("keepalive", "Hyperspace.KeepAlive.Interval");
  alias("lease-interval", "Hyperspace.Lease.Interval");
  alias("grace-period", "Hyperspace.GracePeriod");
  // hidden aliases
  alias("hs-port", "Hyperspace.Replica.Port");
}

void init_hyperspace_command_shell_options() {
  CommandShell::add_options(cmdline_desc());
}

void init_hyperspace_master_options() {
  cmdline_desc().add_options()
    ("port", i16()->default_value(38040),
        "Hyperspace master listening port")
    ("dir", str()->default_value("hyperspace"),
        "Hyperspace root directory name")
    ("keepalive", i32()->default_value(10000),
        "Interval in milliseconds of keepalive message from Hyperspace client")
    ("lease-interval", i32()->default_value(20000),
        "Hyperspace master lease interval in milliseconds")
    ;
  alias("reactors", "Hyperspace.Replica.Reactors");
  alias("workers", "Hyperspace.Replica.Workers");
  alias("port", "Hyperspace.Replica.Port");
  alias("dir", "Hyperspace.Replica.Dir");
  alias("keepalive", "Hyperspace.KeepAlive.Interval");
  alias("lease-interval", "Hyperspace.Lease.Interval");
}

}} // namespace Hypertable::Config
