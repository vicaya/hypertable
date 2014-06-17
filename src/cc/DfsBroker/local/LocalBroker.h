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

#ifndef HYPERTABLE_LOCALBROKER_H
#define HYPERTABLE_LOCALBROKER_H

#include <string>

extern "C" {
#include <unistd.h>
}

#include "Common/atomic.h"
#include "Common/String.h"
#include "Common/atomic.h"
#include "Common/Properties.h"

#include "DfsBroker/Lib/Broker.h"


namespace Hypertable {
  using namespace DfsBroker;

  /**
   *
   */
  class OpenFileDataLocal : public OpenFileData {
  public:
  OpenFileDataLocal(const String &fname, int _fd, int _flags) : fd(_fd), flags(_flags), filename(fname) { }
    virtual ~OpenFileDataLocal() {
      HT_INFOF("close( %s , %d )", filename.c_str(), fd);
      close(fd);
    }
    int  fd;
    int  flags;
    String filename;
  };

  /**
   *
   */
  class OpenFileDataLocalPtr : public OpenFileDataPtr {
  public:
    OpenFileDataLocalPtr() : OpenFileDataPtr() { }
    OpenFileDataLocalPtr(OpenFileDataLocal *ofdl)
      : OpenFileDataPtr(ofdl, true) { }
    OpenFileDataLocal *operator->() const {
      return (OpenFileDataLocal *)get();
    }
  };


  /**
   *
   */
  class LocalBroker : public DfsBroker::Broker {
  public:
    LocalBroker(PropertiesPtr &props);
    virtual ~LocalBroker();

    virtual void open(ResponseCallbackOpen *cb, const char *fname,
                      uint32_t flags, uint32_t bufsz);
    virtual void create(ResponseCallbackOpen *cb, const char *fname, uint32_t flags,
           int32_t bufsz, int16_t replication, int64_t blksz);
    virtual void close(ResponseCallback *cb, uint32_t fd);
    virtual void read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount);
    virtual void append(ResponseCallbackAppend *cb, uint32_t fd,
                        uint32_t amount, const void *data, bool sync);
    virtual void seek(ResponseCallback *cb, uint32_t fd, uint64_t offset);
    virtual void remove(ResponseCallback *cb, const char *fname);
    virtual void length(ResponseCallbackLength *cb, const char *fname);
    virtual void pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset,
                       uint32_t amount);
    virtual void mkdirs(ResponseCallback *cb, const char *dname);
    virtual void rmdir(ResponseCallback *cb, const char *dname);
    virtual void readdir(ResponseCallbackReaddir *cb, const char *dname);
    virtual void flush(ResponseCallback *cb, uint32_t fd);
    virtual void status(ResponseCallback *cb);
    virtual void shutdown(ResponseCallback *cb);
    virtual void exists(ResponseCallbackExists *cb, const char *fname);
    virtual void rename(ResponseCallback *cb, const char *src, const char *dst);
    virtual void debug(ResponseCallback *, int32_t command,
                       StaticBuffer &serialized_parameters);


  private:

    static atomic_t ms_next_fd;

    virtual void report_error(ResponseCallback *cb);

    bool         m_verbose;
    String       m_rootdir;
    bool         m_directio;
  };

}

#endif // HYPERTABLE_LOCALBROKER_H
