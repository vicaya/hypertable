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

namespace hypertable {

  class Comm;

  namespace DfsBroker {

    /**
     * This class serves as the connection handler factory.  An object of this
     * class is registered with the AsyncComm system by passing it as a parameter
     * to the Listen method.  When a connection request arrives, the newInstance
     * method of this class is called which creates an DfsBroker::ConnectionHandler
     * that will be used to service the connection.
     */
    class ConnectionHandlerFactory : public hypertable::ConnectionHandlerFactory {
    public:
      /**
       * Constructor.  Saves a copy of the pointers to the Comm, ApplicationQueue, and Broker 
       * objects which are required in the DfsBroker::ConnectionHandler constructor.
       *
       * @param comm pointer to the AsyncComm object
       * @param appQueue pointer to the application work queue
       * @param broker abstract pointer to the broker object
       */
      ConnectionHandlerFactory(Comm *comm, ApplicationQueuePtr &appQueuePtr, BrokerPtr &brokerPtr) : mComm(comm), mAppQueuePtr(appQueuePtr), mBrokerPtr(brokerPtr) { return; }

      /**
       * Returns a newly constructed DfsBroker::ConnectionHandler object
       */
      virtual void newInstance(DispatchHandlerPtr &dhp) {
	dhp = new ConnectionHandler(mComm, mAppQueuePtr, mBrokerPtr);
      }

    private:
      Comm                *mComm;
      ApplicationQueuePtr  mAppQueuePtr;
      BrokerPtr            mBrokerPtr;
    };

  }

}

#endif // HYPERTABLE_DFSBROKER_CONNECTIONHANDLERFACTORY_H
