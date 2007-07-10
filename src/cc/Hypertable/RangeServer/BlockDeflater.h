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

#ifndef HYPERTABLE_BLOCKDEFLATER_H
#define HYPERTABLE_BLOCKDEFLATER_H

extern "C" {
#include <stdint.h>
}

namespace hypertable {

  class DynamicBuffer;

  class BlockDeflater {
  public:
    virtual ~BlockDeflater() { return; }
    virtual void deflate(hypertable::DynamicBuffer &inbuf, hypertable::DynamicBuffer &outbuf,
			 const char magic[12], size_t reserve=0) = 0;
  };

}

#endif // HYPERTABLE_BLOCKDEFLATER_H
