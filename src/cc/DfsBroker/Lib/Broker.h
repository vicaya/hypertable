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

#ifndef HYPERTABLE_DFSBROKER_BROKER_H
#define HYPERTABLE_DFSBROKER_BROKER_H

#include "DfsBroker/Lib/OpenFileMap.h"

#include "ResponseCallbackOpen.h"
#include "ResponseCallbackRead.h"
#include "ResponseCallbackAppend.h"
#include "ResponseCallbackLength.h"
#include "ResponseCallbackReaddir.h"

using namespace hypertable;

namespace hypertable {

  namespace DfsBroker {

    class Broker {
    public:
      virtual ~Broker() { return; }
      virtual void Open(ResponseCallbackOpen *cb, const char *fieName, uint32_t bufferSize) = 0;
      virtual void Create(ResponseCallbackOpen *cb, const char *fileName, bool overwrite,
			  uint32_t bufferSize, uint16_t replication, uint64_t blockSize) = 0;
      virtual void Close(ResponseCallback *cb, uint32_t fd) = 0;
      virtual void Read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount) = 0;
      virtual void Append(ResponseCallbackAppend *cb, uint32_t fd, uint32_t amount, uint8_t *data) = 0;
      virtual void Seek(ResponseCallback *cb, uint32_t fd, uint64_t offset) = 0;
      virtual void Remove(ResponseCallback *cb, const char *fileName) = 0;
      virtual void Length(ResponseCallbackLength *cb, const char *fieName) = 0;
      virtual void Pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset, uint32_t amount) = 0;
      virtual void Mkdirs(ResponseCallback *cb, const char *dirName) = 0;
      virtual void Rmdir(ResponseCallback *cb, const char *dirName) = 0;
      virtual void Readdir(ResponseCallbackReaddir *cb, const char *dirName) = 0;
      virtual void Flush(ResponseCallback *cb, uint32_t fd) = 0;
      virtual void Status(ResponseCallback *cb) = 0;
      virtual void Shutdown(ResponseCallback *cb) = 0;

      OpenFileMap &GetOpenFileMap() { return mOpenFileMap; }

    protected:
      OpenFileMap mOpenFileMap;
    };
  }
}

#endif // HYPERTABLE_DFSBROKER_BROKER_H
