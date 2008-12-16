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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_DFSBROKER_BROKER_H
#define HYPERTABLE_DFSBROKER_BROKER_H

#include "Common/ReferenceCount.h"
#include "Common/StaticBuffer.h"

#include "DfsBroker/Lib/OpenFileMap.h"

#include "ResponseCallbackOpen.h"
#include "ResponseCallbackRead.h"
#include "ResponseCallbackAppend.h"
#include "ResponseCallbackLength.h"
#include "ResponseCallbackReaddir.h"
#include "ResponseCallbackExists.h"


namespace Hypertable {

  namespace DfsBroker {

    class Broker : public ReferenceCount {
    public:
      virtual ~Broker() { return; }
      virtual void open(ResponseCallbackOpen *, const char *fname,
                        uint32_t bufsz) = 0;
      virtual void create(ResponseCallbackOpen *, const char *fname,
                          bool overwrite, int32_t bufsz,
                          int16_t replication, int64_t blksz) = 0;
      virtual void close(ResponseCallback *, uint32_t fd) = 0;
      virtual void read(ResponseCallbackRead *, uint32_t fd,
                        uint32_t amount) = 0;
      virtual void append(ResponseCallbackAppend *, uint32_t fd,
                          uint32_t amount, const void *data, bool flush) = 0;
      virtual void seek(ResponseCallback *, uint32_t fd, uint64_t offset) = 0;
      virtual void remove(ResponseCallback *, const char *fname) = 0;
      virtual void length(ResponseCallbackLength *, const char *fname) = 0;
      virtual void pread(ResponseCallbackRead *, uint32_t fd, uint64_t offset,
                         uint32_t amount) = 0;
      virtual void mkdirs(ResponseCallback *, const char *dname) = 0;
      virtual void rmdir(ResponseCallback *, const char *dname) = 0;
      virtual void readdir(ResponseCallbackReaddir *, const char *dname) = 0;
      virtual void flush(ResponseCallback *, uint32_t fd) = 0;
      virtual void status(ResponseCallback *) = 0;
      virtual void shutdown(ResponseCallback *) = 0;
      virtual void exists(ResponseCallbackExists *, const char *fname) = 0;
      virtual void rename(ResponseCallback *, const char *src,
                          const char *dst) = 0;
      virtual void debug(ResponseCallback *, int32_t command,
                         StaticBuffer &serialized_parameters) = 0;

      OpenFileMap &get_open_file_map() { return m_open_file_map; }

    protected:
      OpenFileMap m_open_file_map;
    };
    typedef boost::intrusive_ptr<Broker> BrokerPtr;

  }
}

#endif // HYPERTABLE_DFSBROKER_BROKER_H
