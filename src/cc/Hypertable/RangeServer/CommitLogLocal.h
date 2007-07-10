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


#ifndef HYPERTABLE_COMMITLOGLOCAL_H
#define HYPERTABLE_COMMITLOGLOCAL_H

#include <string>

#include "CommitLog.h"

namespace hypertable {

  /**
   *
   */
  class CommitLogLocal : public CommitLog {
  public:
    CommitLogLocal(std::string &logDirRoot, std::string &logDir, int64_t logFileSize);
    virtual int create(std::string &name, int32_t *fdp);
    virtual int write(int32_t fd, const void *buf, uint32_t amount);
    virtual int close(int32_t fd);
    virtual int sync(int32_t fd);
    virtual int unlink(std::string &name);
  };
}

#endif // HYPERTABLE_COMMITLOGLOCAL_H

