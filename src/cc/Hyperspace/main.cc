/**
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
#include <iostream>
#include <fstream>
#include <string>

extern "C" {
#include <poll.h>
#include <sys/types.h>
#include <unistd.h>
}

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionHandlerFactory.h"

#include "Config.h"
#include "ServerConnectionHandler.h"
#include "ServerKeepaliveHandler.h"
#include "Master.h"

using namespace Hyperspace;
using namespace Hypertable;
using namespace Config;
using namespace std;


/**
 * Handler factory for Hyperspace master
 */
class HandlerFactory : public ConnectionHandlerFactory {

public:
  HandlerFactory(Comm *comm, ApplicationQueuePtr &app_queue, MasterPtr &master)
    : m_comm(comm), m_app_queue_ptr(app_queue), m_master_ptr(master) { }

  virtual void get_instance(DispatchHandlerPtr &dhp) {
    dhp = new ServerConnectionHandler(m_comm, m_app_queue_ptr, m_master_ptr);
  }

private:
  Comm                 *m_comm;
  ApplicationQueuePtr   m_app_queue_ptr;
  MasterPtr             m_master_ptr;
};


int main(int argc, char **argv) {
  typedef Cons<HyperspaceMasterPolicy, DefaultServerPolicy> AppPolicy;

  try {
    init_with_policy<AppPolicy>(argc, argv);

    Comm *comm = Comm::instance();
    ConnectionManagerPtr conn_mgr = new ConnectionManager(comm);
    ServerKeepaliveHandlerPtr keepalive_handler;
    ApplicationQueuePtr app_queue_ptr;
    MasterPtr master = new Master(conn_mgr, properties,
                                  keepalive_handler, app_queue_ptr);
    int worker_count = get_i32("workers");
    uint16_t port = get_i16("port");
    InetAddr local_addr(INADDR_ANY, port);
    ConnectionHandlerFactoryPtr hf(new HandlerFactory(comm, app_queue_ptr, master));

    comm->listen(local_addr, hf);

    DispatchHandlerPtr dhp(keepalive_handler.get());
    // give hyperspace message higher priority if possible
    comm->create_datagram_receive_socket(&local_addr, 0x10, dhp);

    app_queue_ptr->join();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
