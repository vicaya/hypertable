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

