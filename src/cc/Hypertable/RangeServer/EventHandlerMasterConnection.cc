/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include <iostream>
#include <poll.h>

#include <boost/algorithm/string.hpp>

#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/String.h"
#include "Common/System.h"
#include "Common/SystemInfo.h"

#include "Hypertable/Lib/MasterClient.h"

#include "Location.h"
#include "EventHandlerMasterConnection.h"

using namespace Hypertable;
using namespace Config;
using namespace std;


EventHandlerMasterConnection::EventHandlerMasterConnection(MasterClientPtr &master, EventPtr &event) :
  ApplicationHandler(event), m_master(master), m_location_persisted(false) {

  m_location_file = System::install_dir + "/run/location";

  // Get location string
  {
    String location = properties->get_str("Hypertable.RangeServer.ProxyName");
    if (!location.empty()) {
      boost::trim(location);
      Location::set(location);
    }
    else if (FileUtils::exists(m_location_file)) {
      if (FileUtils::read(m_location_file, location) <= 0) {
        HT_ERRORF("Problem reading location file '%s'", m_location_file.c_str());
        _exit(1);
      }
      m_location_persisted = true;
      boost::trim(location);
      Location::set(location);
    }
  }
  uint64_t port = properties->get_i16("Hypertable.RangeServer.Port");

  m_inet_addr = InetAddr(System::net_info().primary_addr, port);

}


void EventHandlerMasterConnection::run() {

  while (true) {
    String location = Location::get();
    try {
      m_master->register_server(location, m_inet_addr);
      if (!m_location_persisted) {
	Location::set(location);
	location += "\n";
	if (FileUtils::write(m_location_file, location) < 0) {
	  HT_ERRORF("Unable to write location to file '%s'", m_location_file.c_str());
	  _exit(1);
	}
	m_location_persisted = true;
      }
      break;
    }
    catch (Hypertable::Exception &e) {
      HT_ERRORF("Problem registering ourselves (location=\"%s\") with the Master - %s - %s",
                location.c_str(), Error::get_text(e.code()), e.what());
    }
    poll(0, 0, 1000);
  }

}
