/**
 * Copyright (C) 2008 Donald <donaldliew@gmail.com>
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include <cstdlib>
#include <iostream>

extern "C" {
#include <netdb.h>
}

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/ReactorFactory.h"

#include "DfsBroker/Lib/Client.h"

#include "Hyperspace/Session.h"
#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/RangeServerClient.h"

#include "Hypertable/Lib/Stat.h"

using namespace Hypertable;
using namespace Config;

int main(int argc, char **argv) {
  typedef Cons<RangeServerClientPolicy, DefaultCommPolicy> AppPolicy;

  try {
    Comm *comm = Comm::instance();
    ConnectionManagerPtr conn_mgr = new ConnectionManager(comm);
    InetAddr addr(get_str("rs-host"), get_i16("rs-port"));
    int timeout = get_i32("timeout");

    conn_mgr->add(addr, timeout, "Range Server");

    if (!conn_mgr->wait_for_connection(addr, timeout))
      HT_THROWF(Error::REQUEST_TIMEOUT, "connecting to range server %s",
                addr.format().c_str());

    RangeServerClient *client = new RangeServerClient(comm, timeout);
    RangeServerStat stat;

    client->get_statistics(addr, stat);

    std::cout << stat << std::endl;
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
  return 0;
}
