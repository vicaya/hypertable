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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_DFSBROKER_CONNECTIONHANDLERFACTORY_H
#define HYPERTABLE_DFSBROKER_CONNECTIONHANDLERFACTORY_H

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/ConnectionHandlerFactory.h"
#include "AsyncComm/DispatchHandler.h"

#include "ConnectionHandler.h"
#include "Broker.h"

namespace Hypertable {

  class Comm;

  namespace DfsBroker {

    /**
     * This class serves as the connection handler factory.  An object of this
     * class is registered with the AsyncComm system by passing it as a parameter
     * to the Listen method.  When a connection request arrives, the newInstance
     * method of this class is called which creates an DfsBroker::connection_handler
     * that will be used to service the connection.
     */
    class ConnectionHandlerFactory : public Hypertable::ConnectionHandlerFactory {
    public:
      /**
       * Constructor.  Saves a copy of the pointers to the Comm, ApplicationQueue, and Broker 
       * objects which are required in the DfsBroker::connection_handler constructor.
       *
       * @param comm pointer to the AsyncComm object
       * @param appQueuePtr pointer to the application work queue
       * @param brokerPtr abstract pointer to the broker object
       */
      ConnectionHandlerFactory(Comm *comm, ApplicationQueuePtr &appQueuePtr, BrokerPtr &brokerPtr) : m_comm(comm), m_app_queue_ptr(appQueuePtr), m_broker_ptr(brokerPtr) { return; }

      /**
       * Returns a newly constructed DfsBroker::connection_handler object
       */
      virtual void get_instance(DispatchHandlerPtr &dhp) {
	dhp = new ConnectionHandler(m_comm, m_app_queue_ptr, m_broker_ptr);
      }

    private:
      Comm                *m_comm;
      ApplicationQueuePtr  m_app_queue_ptr;
      BrokerPtr            m_broker_ptr;
    };

  }

}

#endif // HYPERTABLE_DFSBROKER_CONNECTIONHANDLERFACTORY_H
