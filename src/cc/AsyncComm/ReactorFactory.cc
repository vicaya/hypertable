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

#include "ReactorFactory.h"
#include "ReactorRunner.h"
using namespace hypertable;

#include <cassert>

extern "C" {
#include <signal.h>
}

vector<Reactor *> ReactorFactory::msReactors;
atomic_t ReactorFactory::msNextReactor = ATOMIC_INIT(0);



/**
 * Method to initialize the reactor factory
 *
 * @param reactorCount number of reactors to initialize
 */
void ReactorFactory::Initialize(uint16_t reactorCount) {
  Reactor *reactor;
  ReactorRunner rrunner;
  signal(SIGPIPE, SIG_IGN);
  assert(reactorCount > 0);
  for (uint16_t i=0; i<reactorCount; i++) {
    reactor = new Reactor();
    msReactors.push_back(reactor);
    rrunner.SetReactor(reactor);
    reactor->threadPtr = new boost::thread(rrunner);
  }
}

