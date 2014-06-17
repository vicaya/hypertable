/** -*- C++ -*-
 * Copyright (C) 2009 Gregory Farnum (gfarnum@gmail.com)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef HYPERTABLE_CEPHBROKER_H
#define HYPERTABLE_CEPHBROKER_H

extern "C" {
#include <unistd.h>
}

#include <ceph/libceph.h>

#include "Common/String.h"
#include "Common/atomic.h"
#include "Common/Properties.h"

#include "DfsBroker/Lib/Broker.h"

namespace Hypertable {
  using namespace DfsBroker;
  /**
   *
   */
  class OpenFileDataCeph : public OpenFileData {
  public:
    OpenFileDataCeph(const String& fname, int _fd, int _flags) :
      fd(_fd), flags(_flags), filename(fname) {}
    virtual ~OpenFileDataCeph() { ceph_close(fd); }
    int fd;
    int flags;
    String filename;
  };

  /**
   *
   */
  class OpenFileDataCephPtr : public OpenFileDataPtr {
  public:
    OpenFileDataCephPtr() : OpenFileDataPtr() { }
    OpenFileDataCephPtr(OpenFileDataCeph *ofdl) : OpenFileDataPtr(ofdl, true) { }
    OpenFileDataCeph *operator->() const { return (OpenFileDataCeph *)get(); }
  };

  /**
   *
   */
  class CephBroker : public DfsBroker::Broker {
  public:
    CephBroker(PropertiesPtr& cfg);
    virtual ~CephBroker();

    virtual void open(ResponseCallbackOpen *cb, const char *fname,
                      uint32_t flags, uint32_t bufsz);
    virtual void
    create(ResponseCallbackOpen *cb, const char *fname, uint32_t flags,
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
    virtual void flush(ResponseCallback *cb, uint32_t fd);
    virtual void status(ResponseCallback *cb);
    virtual void shutdown(ResponseCallback *cb);
    virtual void readdir(ResponseCallbackReaddir *cb, const char *dname);
    virtual void exists(ResponseCallbackExists *cb, const char *fname);
    virtual void rename(ResponseCallback *cb, const char *src, const char *dst);
    virtual void debug(ResponseCallback *, int32_t command,
                       StaticBuffer &serialized_parameters);

  private:
    static atomic_t ms_next_fd;

    virtual void report_error(ResponseCallback *cb, int error);

    void make_abs_path(const char *fname, String& abs) {
      if (fname[0] == '/')
	abs = fname;
      else
	abs = m_root_dir + "/" + fname;
    }

    int rmdir_recursive(const char *directory);

    bool m_verbose;
    String m_root_dir;
  };
}

#endif //HYPERTABLE_CEPH_BROKER_H
