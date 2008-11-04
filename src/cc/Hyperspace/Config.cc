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
    ("hyperspace-timeout", i32()->default_value(30),
        "Timeout in seconds for hyperspace connection")
    ("keepalive", i32()->default_value(10),
        "Interval in seconds of keepalive message from Hyperspace client")
    ("lease-interval", i32()->default_value(60),
        "Hyperspace master lease interval in seconds")
    ("grace-period", i32()->default_value(60),
        "Grace period in seconds before 'jeopardy' for hyperspace client")
    ;
  alias("hyperspace-timeout", "Hyperspace.Timeout");
  alias("keepalive", "Hyperspace.KeepAlive.Interval");
  alias("lease-interval", "Hyperspace.Lease.Interval");
  alias("grace-period", "Hyperspace.GracePeriod");
  // hidden aliases
  alias("hs-host", "Hyperspace.Master.Host");
  alias("hs-port", "Hyperspace.Master.Port");
}

void init_hyperspace_client() {
  Endpoint e = InetAddr::parse_endpoint(get_str("hyperspace"));
  bool isdefaulted = defaulted("hyperspace");
  properties->set("hs-host", e.host, isdefaulted);
  properties->set("hs-port", e.port, !e.port || isdefaulted);
}

void init_hyperspace_master_options() {
  cmdline_desc().add_options()
    ("port", i16()->default_value(38040),
        "Hyperspace master listening port")
    ("dir", str()->default_value("hyperspace"),
        "Hyperspace root directory name")
    ("keepalive", i32()->default_value(10),
        "Interval in seconds of keepalive message from Hyperspace client")
    ("lease-interval", i32()->default_value(20),
        "Hyperspace master lease interval in seconds")
    ;
  alias("reactors", "Hyperspace.Master.Reactors");
  alias("workers", "Hyperspace.Master.Workers");
  alias("port", "Hyperspace.Master.Port");
  alias("dir", "Hyperspace.Master.Dir");
  alias("keepalive", "Hyperspace.KeepAlive.Interval");
  alias("lease-interval", "Hyperspace.Lease.Interval");
}

}} // namespace Hypertable::Config
