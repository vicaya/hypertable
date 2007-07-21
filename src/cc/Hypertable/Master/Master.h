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

#ifndef HYPERTABLE_MASTER_H
#define HYPERTABLE_MASTER_H

#include "Common/Properties.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ResponseCallback.h"

#include "Hypertable/HdfsClient/HdfsClient.h"
#include "Hyperspace/HyperspaceClient.h"

#include "ResponseCallbackGetSchema.h"

using namespace hypertable;

namespace hypertable {

  class Master {
  public:
    Master(Comm *comm, Properties *props);
    ~Master();
    void CreateTable(ResponseCallback *cb, const char *tableName, const char *schemaString);
    void GetSchema(ResponseCallbackGetSchema *cb, const char *tableName);
    bool CreateDirectoryLayout();

  private:
    Comm *mComm;
    bool mVerbose;
    HyperspaceClient *mHyperspace;
    HdfsClient *mHdfsClient;
    atomic_t mLastTableId;
  };

}

#endif // HYPERTABLE_MASTER_H
