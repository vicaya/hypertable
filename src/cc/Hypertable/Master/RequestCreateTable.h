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


#ifndef HYPERTABLE_REQUESTCREATETABLE_H
#define HYPERTABLE_REQUESTCREATETABLE_H

#include "Common/Runnable.h"
#include "AsyncComm/Event.h"

using namespace hypertable;

namespace hypertable {

  class RequestCreateTable : public Runnable {
  public:
    RequestCreateTable(Event &event) : mEvent(event) {
      return;
    }

    virtual void run();

  private:
    Event mEvent;
  };

}

#endif // HYPERTABLE_REQUESTCREATETABLE_H
