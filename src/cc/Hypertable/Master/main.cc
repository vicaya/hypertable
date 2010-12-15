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
#include <fstream>
#include <string>

extern "C" {
#include <poll.h>
#include <sys/types.h>
#include <unistd.h>
}

#include "Common/Init.h"
#include "Common/InetAddr.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionHandlerFactory.h"

#include "Hypertable/Lib/Config.h"

#include "ConnectionHandler.h"
#include "Master.h"

using namespace Hypertable;
using namespace Config;
using namespace std;


namespace {

  struct MyPolicy : Config::Policy {
    static void init_options() {
      alias("port", "Hypertable.Master.Port");
      alias("workers", "Hypertable.Master.Workers");
      alias("reactors", "Hypertable.Master.Reactors");
    }
  };

  typedef Meta::list<MyPolicy, GenericServerPolicy, DfsClientPolicy,
          HyperspaceClientPolicy, DefaultCommPolicy> Policies;

  class HandlerFactory : public ConnectionHandlerFactory {
  public:
    HandlerFactory(Comm *comm, ApplicationQueuePtr &app_queue,
                   MasterPtr &master)
      : m_comm(comm), m_app_queue(app_queue), m_master(master) {
      m_handler = new ConnectionHandler(m_comm, m_app_queue, m_master);
    }

    virtual void get_instance(DispatchHandlerPtr &dhp) {
      dhp = m_handler;
    }

  private:
    Comm                *m_comm;
    ApplicationQueuePtr  m_app_queue;
    MasterPtr            m_master;
    DispatchHandlerPtr   m_handler;
  };

} // local namespace


int main(int argc, char **argv) {

  // Register ourselves as the Comm-laer proxy master
  ReactorFactory::proxy_master = true;

  try {
    init_with_policies<Policies>(argc, argv);

    uint16_t port = get_i16("port");
    int worker_count  = get_i32("workers");

    Comm *comm = Comm::instance();
    ConnectionManagerPtr conn_mgr = new ConnectionManager(comm);
    ApplicationQueuePtr app_queue = new ApplicationQueue(worker_count);
    MasterPtr master = new Master(properties, conn_mgr, app_queue);
    ConnectionHandlerFactoryPtr hf(new HandlerFactory(comm, app_queue, master));
    InetAddr listen_addr(INADDR_ANY, port);

    comm->listen(listen_addr, hf);

    master->join();
    comm->close_socket(listen_addr);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
