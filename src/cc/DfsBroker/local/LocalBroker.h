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

#ifndef HYPERTABLE_LOCALBROKER_H
#define HYPERTABLE_LOCALBROKER_H

#include <string>

extern "C" {
#include <unistd.h>  
}

#include "Common/atomic.h"
#include "Common/Properties.h"

#include "DfsBroker/Lib/Broker.h"

using namespace Hypertable;
using namespace Hypertable::DfsBroker;

namespace Hypertable {

  /**
   *
   */
  class OpenFileDataLocal : public OpenFileData {
  public:
    OpenFileDataLocal(int _fd, int _flags) : fd(_fd), flags(_flags) { return; }
    virtual ~OpenFileDataLocal() { close(fd); }
    int  fd;
    int  flags;
  };

  /**
   * 
   */
  class OpenFileDataLocalPtr : public OpenFileDataPtr {
  public:
    OpenFileDataLocalPtr() : OpenFileDataPtr() { return; }
    OpenFileDataLocalPtr(OpenFileDataLocal *ofdl) : OpenFileDataPtr(ofdl, true) { return; }
    OpenFileDataLocal *operator->() const {
      return (OpenFileDataLocal *)get();
    }
  };
  

  /**
   * 
   */
  class LocalBroker : public DfsBroker::Broker {
  public:
    LocalBroker(PropertiesPtr &propsPtr);
    virtual ~LocalBroker();

    virtual void open(ResponseCallbackOpen *cb, const char *fileName, uint32_t bufferSize);
    virtual void create(ResponseCallbackOpen *cb, const char *fileName, bool overwrite,
			uint32_t bufferSize, uint16_t replication, uint64_t blockSize);
    virtual void close(ResponseCallback *cb, uint32_t fd);
    virtual void read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount);
    virtual void append(ResponseCallbackAppend *cb, uint32_t fd, uint32_t amount, uint8_t *data);
    virtual void seek(ResponseCallback *cb, uint32_t fd, uint64_t offset);
    virtual void remove(ResponseCallback *cb, const char *fileName);
    virtual void length(ResponseCallbackLength *cb, const char *fileName);
    virtual void pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset, uint32_t amount);
    virtual void mkdirs(ResponseCallback *cb, const char *dirName);
    virtual void rmdir(ResponseCallback *cb, const char *dirName);
    virtual void readdir(ResponseCallbackReaddir *cb, const char *dirName);
    virtual void flush(ResponseCallback *cb, uint32_t fd);
    virtual void status(ResponseCallback *cb);
    virtual void shutdown(ResponseCallback *cb);

  private:

    virtual void report_error(ResponseCallback *cb);

    bool         m_verbose;
    std::string  m_rootdir;
  };

}

#endif // HYPERTABLE_LOCALBROKER_H
