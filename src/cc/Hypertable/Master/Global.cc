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


#include "Global.h"

using namespace hypertable;

Comm             *Global::comm = 0;
Properties       *Global::props = 0;
WorkQueue        *Global::workQueue;
HyperspaceClient *Global::hyperspace = 0;
HdfsClient       *Global::hdfsClient = 0;
MasterProtocol   *Global::protocol = 0;
atomic_t          Global::lastTableId = ATOMIC_INIT(1);
bool              Global::verbose = false;
std::vector< boost::shared_ptr<RangeServerInfoT> > Global::rangeServerInfo;
atomic_t          Global::nextServer = ATOMIC_INIT(0);
