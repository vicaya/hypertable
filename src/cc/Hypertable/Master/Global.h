/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef HYPERTABLE_MASTER_GLOBAL_H
#define HYPERTABLE_MASTER_GLOBAL_H

#include <string>
#include <vector>

extern "C" {
#include <netdb.h>
}

#include <boost/shared_ptr.hpp>

#include "Common/atomic.h"
#include "Common/Properties.h"
#include "Common/WorkQueue.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"

#include "Hypertable/HdfsClient/HdfsClient.h"
#include "MasterProtocol.h"
#include "Hyperspace/HyperspaceClient.h"

namespace hypertable {

  typedef struct {
    std::string hostname;
    struct sockaddr_in addr;
    ConnectionManager *connManager;
  } RangeServerInfoT;

  class Global {
  public:
    static hypertable::Comm *comm;
    static hypertable::Properties *props;
    static hypertable::WorkQueue  *workQueue;
    static hypertable::HyperspaceClient *hyperspace;
    static HdfsClient *hdfsClient;
    static MasterProtocol *protocol;
    static atomic_t lastTableId;
    static bool verbose;
    static std::vector< boost::shared_ptr<RangeServerInfoT> > rangeServerInfo;
    static atomic_t nextServer;
  };
}

#endif // HYPERTABLE_MASTER_GLOBAL_H

