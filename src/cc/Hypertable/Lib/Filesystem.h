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

#ifndef HYPERTABLE_FILESYSTEM_H
#define HYPERTABLE_FILESYSTEM_H

#include "AsyncComm/DispatchHandler.h"

namespace hypertable {

  class Filesystem {
  public:

    virtual int Open(const char *name, DispatchHandler *handler, uint32_t *msgIdp) = 0;
    virtual int Open(const char *name, int32_t *fdp) = 0;

    virtual int Create(const char *name, bool overwrite, int32_t bufferSize,
		       int32_t replication, int64_t blockSize, DispatchHandler *handler, uint32_t *msgIdp) = 0;
    virtual int Create(const char *name, bool overwrite, int32_t bufferSize,
		       int32_t replication, int64_t blockSize, int32_t *fdp) = 0;

    virtual int Close(int32_t fd, DispatchHandler *handler, uint32_t *msgIdp) = 0;
    virtual int Close(int32_t fd) = 0;

    virtual int Read(int32_t fd, uint32_t amount, DispatchHandler *handler, uint32_t *msgIdp) = 0;
    virtual int Read(int32_t fd, uint32_t amount, uint8_t *dst, uint32_t *nreadp) = 0;

    virtual int Append(int32_t fd, uint8_t *buf, uint32_t amount, DispatchHandler *handler, uint32_t *msgIdp) = 0;
    virtual int Append(int32_t fd, uint8_t *buf, uint32_t amount) = 0;

    virtual int Seek(int32_t fd, uint64_t offset, DispatchHandler *handler, uint32_t *msgIdp) = 0;
    virtual int Seek(int32_t fd, uint64_t offset) = 0;

    virtual int Remove(const char *name, DispatchHandler *handler, uint32_t *msgIdp) = 0;
    virtual int Remove(const char *name) = 0;

    virtual int Length(const char *name, DispatchHandler *handler, uint32_t *msgIdp) = 0;
    virtual int Length(const char *name, int64_t *lenp) = 0;

    virtual int Pread(int32_t fd, uint64_t offset, uint32_t amount, DispatchHandler *handler, uint32_t *msgIdp) = 0;
    virtual int Pread(int32_t fd, uint64_t offset, uint32_t amount, uint8_t *dst, uint32_t *nreadp) = 0;

    virtual int Mkdirs(const char *name, DispatchHandler *handler, uint32_t *msgIdp) = 0;
    virtual int Mkdirs(const char *name) = 0;

  };

}


#endif // HYPERTABLE_FILESYSTEM_H

