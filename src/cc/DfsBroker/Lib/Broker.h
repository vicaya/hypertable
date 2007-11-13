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

#include "Common/ReferenceCount.h"

#include "DfsBroker/Lib/OpenFileMap.h"

#include "ResponseCallbackOpen.h"
#include "ResponseCallbackRead.h"
#include "ResponseCallbackAppend.h"
#include "ResponseCallbackLength.h"
#include "ResponseCallbackReaddir.h"

using namespace Hypertable;

namespace Hypertable {

  namespace DfsBroker {

    class Broker : public ReferenceCount {
    public:
      virtual ~Broker() { return; }
      virtual void open(ResponseCallbackOpen *cb, const char *fieName, uint32_t bufferSize) = 0;
      virtual void create(ResponseCallbackOpen *cb, const char *fileName, bool overwrite,
			  uint32_t bufferSize, uint16_t replication, uint64_t blockSize) = 0;
      virtual void close(ResponseCallback *cb, uint32_t fd) = 0;
      virtual void read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount) = 0;
      virtual void append(ResponseCallbackAppend *cb, uint32_t fd, uint32_t amount, uint8_t *data) = 0;
      virtual void seek(ResponseCallback *cb, uint32_t fd, uint64_t offset) = 0;
      virtual void remove(ResponseCallback *cb, const char *fileName) = 0;
      virtual void length(ResponseCallbackLength *cb, const char *fieName) = 0;
      virtual void pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset, uint32_t amount) = 0;
      virtual void mkdirs(ResponseCallback *cb, const char *dirName) = 0;
      virtual void rmdir(ResponseCallback *cb, const char *dirName) = 0;
      virtual void readdir(ResponseCallbackReaddir *cb, const char *dirName) = 0;
      virtual void flush(ResponseCallback *cb, uint32_t fd) = 0;
      virtual void status(ResponseCallback *cb) = 0;
      virtual void shutdown(ResponseCallback *cb) = 0;

      OpenFileMap &get_open_file_map() { return m_open_file_map; }

    protected:
      OpenFileMap m_open_file_map;
    };
    typedef boost::intrusive_ptr<Broker> BrokerPtr;
    
  }
}

#endif // HYPERTABLE_DFSBROKER_BROKER_H
