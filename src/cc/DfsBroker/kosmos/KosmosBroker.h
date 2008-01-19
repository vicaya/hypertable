/**
 * Copyright (C) 2007 Sriram Rao (Kosmix Corp.)
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

#ifndef HYPERTABLE_KFSBROKER_H
#define HYPERTABLE_KFSBROKER_H

#include <string>

extern "C" {
#include <unistd.h>  
}

#include "Common/atomic.h"
#include "Common/Properties.h"

#include "DfsBroker/Lib/Broker.h"

#include "kfs/KfsClient.h"

using namespace Hypertable;
using namespace Hypertable::DfsBroker;

namespace Hypertable {

  /**
   *
   */
  class OpenFileDataKosmos : public OpenFileData {
  public:
    OpenFileDataKosmos(int _fd, int _flags) : fd(_fd), flags(_flags) { return; }
    virtual ~OpenFileDataKosmos() { 
      KFS::KfsClient *clnt = KFS::KfsClient::Instance();
      clnt->Close(fd);
    }
    int  fd;
    int  flags;
  };

  /**
   * 
   */
  class OpenFileDataKosmosPtr : public OpenFileDataPtr {
  public:
    OpenFileDataKosmosPtr() : OpenFileDataPtr() { return; }
    OpenFileDataKosmosPtr(OpenFileDataKosmos *ofdl) : OpenFileDataPtr(ofdl, true) { return; }
    OpenFileDataKosmos *operator->() const {
      return (OpenFileDataKosmos *)get();
    }
  };
  

  /**
   * 
   */
  class KosmosBroker : public DfsBroker::Broker {
  public:
    KosmosBroker(PropertiesPtr &propsPtr);
    virtual ~KosmosBroker();

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
    virtual void flush(ResponseCallback *cb, uint32_t fd);
    virtual void status(ResponseCallback *cb);
    virtual void shutdown(ResponseCallback *cb);
    virtual void readdir(ResponseCallbackReaddir *cb, const char *dirName);

  private:

    virtual void ReportError(ResponseCallback *cb, int error);

    bool         mVerbose;
    std::string  mRootdir;
  };

}

#endif // HYPERTABLE_KFSBROKER_H
