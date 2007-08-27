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
#include "DfsBroker/Lib/OpenFileMap.h"

using namespace hypertable;
using namespace hypertable::DfsBroker;

namespace hypertable {

  /**
   *
   */
  class OpenFileDataLocal : public OpenFileData {
  public:
    OpenFileDataLocal(int _fd, int _flags) : fd(_fd), flags(_flags) { return; }
    ~OpenFileDataLocal() { close(fd); }
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

    virtual void Open(ResponseCallbackOpen *cb, const char *fileName, uint32_t bufferSize);
    virtual void Create(ResponseCallbackOpen *cb, const char *fileName, bool overwrite,
			uint32_t bufferSize, uint16_t replication, uint64_t blockSize);
    virtual void Close(ResponseCallback *cb, uint32_t fd);
    virtual void Read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount);
    virtual void Append(ResponseCallbackAppend *cb, uint32_t fd, uint32_t amount, uint8_t *data);
    virtual void Seek(ResponseCallback *cb, uint32_t fd, uint64_t offset);
    virtual void Remove(ResponseCallback *cb, const char *fileName);
    virtual void Length(ResponseCallbackLength *cb, const char *fileName);
    virtual void Pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset, uint32_t amount);
    virtual void Mkdirs(ResponseCallback *cb, const char *dirName);
    virtual void Flush(ResponseCallback *cb, uint32_t fd);
    virtual void Status(ResponseCallback *cb);
    virtual void Shutdown(ResponseCallback *cb);

  private:

    virtual void ReportError(ResponseCallback *cb);

    bool         mVerbose;
    std::string  mRootdir;
    OpenFileMap  mOpenFileMap;
  };

}

#endif // HYPERTABLE_LOCALBROKER_H
