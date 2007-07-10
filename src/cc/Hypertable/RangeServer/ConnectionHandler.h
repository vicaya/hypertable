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
#ifndef HYPERTABLE_RANGESERVER_CONNECTIONHANDLER_H
#define HYPERTABLE_RANGESERVER_CONNECTIONHANDLER_H

#include "AsyncComm/CallbackHandler.h"

#include "RequestFactory.h"

namespace hypertable {

  /**
   */
  class ConnectionHandler : public CallbackHandler {
  public:

    virtual void handle(Event &event);

  private:
    RequestFactory  mRequestFactory;
  };

}

#endif // HYPERTABLE_RANGESERVER_CONNECTIONHANDLER_H

