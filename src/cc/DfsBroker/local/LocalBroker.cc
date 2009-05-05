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

#include "Common/Compat.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

extern "C" {
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
}

#include "Common/String.h"
#include "Common/FileUtils.h"
#include "Common/System.h"
#include "Common/Filesystem.h"

#include "LocalBroker.h"

using namespace Hypertable;

atomic_t LocalBroker::ms_next_fd = ATOMIC_INIT(0);

LocalBroker::LocalBroker(PropertiesPtr &cfg) {
  m_verbose = cfg->get_bool("verbose");

  /**
   * Determine root directory
   */
  Path root = cfg->get_str("root", "");

  if (!root.is_complete())
    root = Path(System::install_dir) / root;

  m_rootdir = root.directory_string();

  // ensure that root directory exists
  if (!FileUtils::mkdirs(m_rootdir))
    exit(1);
}



LocalBroker::~LocalBroker() {
}


void
LocalBroker::open(ResponseCallbackOpen *cb, const char *fname, uint32_t bufsz) {
  int fd, local_fd;
  String abspath;

  HT_DEBUGF("open file='%s' bufsz=%d", fname, bufsz);

  if (fname[0] == '/')
    abspath = m_rootdir + fname;
  else
    abspath = m_rootdir + "/" + fname;

  fd = atomic_inc_return(&ms_next_fd);

  /**
   * Open the file
   */
  if ((local_fd = ::open(abspath.c_str(), O_RDONLY)) == -1) {
    HT_ERRORF("open failed: file='%s' - %s", abspath.c_str(), strerror(errno));
    report_error(cb);
    return;
  }

  HT_INFOF("open( %s ) = %d", fname, local_fd);

  {
    struct sockaddr_in addr;
    OpenFileDataLocalPtr fdata(new OpenFileDataLocal(fname, local_fd, O_RDONLY));

    cb->get_address(addr);

    m_open_file_map.create(fd, addr, fdata);

    cb->response(fd);
  }
}


void
LocalBroker::create(ResponseCallbackOpen *cb, const char *fname, bool overwrite,
                    int32_t bufsz, int16_t replication, int64_t blksz) {
  int fd, local_fd;
  int flags;
  String abspath;

  HT_DEBUGF("create file='%s' overwrite=%d bufsz=%d replication=%d blksz=%lld",
            fname, (int)overwrite, bufsz, (int)replication, (Lld)blksz);

  if (fname[0] == '/')
    abspath = m_rootdir + fname;
  else
    abspath = m_rootdir + "/" + fname;

  fd = atomic_inc_return(&ms_next_fd);

  if (overwrite)
    flags = O_WRONLY | O_CREAT | O_TRUNC;
  else
    flags = O_WRONLY | O_CREAT | O_APPEND;

  /**
   * Open the file
   */
  if ((local_fd = ::open(abspath.c_str(), flags, 0644)) == -1) {
    HT_ERRORF("open failed: file='%s' - %s", abspath.c_str(), strerror(errno));
    report_error(cb);
    return;
  }

  //HT_DEBUGF("created file='%s' fd=%d local_fd=%d", fname, fd, local_fd);

  HT_INFOF("create( %s ) = %d", fname, local_fd);

  {
    struct sockaddr_in addr;
    OpenFileDataLocalPtr fdata(new OpenFileDataLocal(fname, local_fd, O_WRONLY));

    cb->get_address(addr);

    m_open_file_map.create(fd, addr, fdata);

    cb->response(fd);
  }
}


void LocalBroker::close(ResponseCallback *cb, uint32_t fd) {
  HT_DEBUGF("close fd=%d", fd);
  m_open_file_map.remove(fd);
  cb->response_ok();
}


void LocalBroker::read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount) {
  OpenFileDataLocalPtr fdata;
  ssize_t nread;
  uint64_t offset;
  StaticBuffer buf(new uint8_t [amount], amount);

  HT_DEBUGF("read fd=%d amount=%d", fd, amount);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    HT_ERRORF("bad file handle: %d", fd);
    return;
  }

  if ((offset = (uint64_t)lseek(fdata->fd, 0, SEEK_CUR)) == (uint64_t)-1) {
    HT_ERRORF("lseek failed: fd=%d offset=0 SEEK_CUR - %s", fdata->fd,
              strerror(errno));
    report_error(cb);
    return;
  }

  if ((nread = FileUtils::read(fdata->fd, buf.base, amount)) == -1) {
    HT_ERRORF("read failed: fd=%d amount=%d - %s", fdata->fd, amount,
              strerror(errno));
    report_error(cb);
    return;
  }

  buf.size = nread;

  cb->response(offset, buf);
}


void LocalBroker::append(ResponseCallbackAppend *cb, uint32_t fd,
                         uint32_t amount, const void *data, bool sync) {
  OpenFileDataLocalPtr fdata;
  ssize_t nwritten;
  uint64_t offset;

  HT_DEBUG_OUT <<"append fd="<< fd <<" amount="<< amount <<" data='"
      << format_bytes(20, data, amount) <<" sync="<< sync << HT_END;

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((offset = (uint64_t)lseek(fdata->fd, 0, SEEK_CUR)) == (uint64_t)-1) {
    HT_ERRORF("lseek failed: fd=%d offset=0 SEEK_CUR - %s", fdata->fd,
              strerror(errno));
    report_error(cb);
    return;
  }

  if ((nwritten = FileUtils::write(fdata->fd, data, amount)) == -1) {
    HT_ERRORF("write failed: fd=%d amount=%d - %s", fdata->fd, amount,
              strerror(errno));
    report_error(cb);
    return;
  }

  if (sync && fsync(fdata->fd) != 0) {
    HT_ERRORF("flush failed: fd=%d - %s", fdata->fd, strerror(errno));
    report_error(cb);
    return;
  }

  cb->response(offset, nwritten);
}


void LocalBroker::seek(ResponseCallback *cb, uint32_t fd, uint64_t offset) {
  OpenFileDataLocalPtr fdata;

  HT_DEBUGF("seek fd=%lu offset=%llu", (Lu)fd, (Llu)offset);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((offset = (uint64_t)lseek(fdata->fd, offset, SEEK_SET)) == (uint64_t)-1) {
    HT_ERRORF("lseek failed: fd=%d offset=%llu - %s", fdata->fd, (Llu)offset,
              strerror(errno));
    report_error(cb);
    return;
  }

  cb->response_ok();
}


void LocalBroker::remove(ResponseCallback *cb, const char *fname) {
  String abspath;

  HT_DEBUGF("remove file='%s'", fname);

  if (fname[0] == '/')
    abspath = m_rootdir + fname;
  else
    abspath = m_rootdir + "/" + fname;

  if (unlink(abspath.c_str()) == -1) {
    HT_ERRORF("unlink failed: file='%s' - %s", abspath.c_str(),
              strerror(errno));
    report_error(cb);
    return;
  }

  cb->response_ok();
}


void LocalBroker::length(ResponseCallbackLength *cb, const char *fname) {
  String abspath;
  uint64_t length;

  HT_DEBUGF("length file='%s'", fname);

  if (fname[0] == '/')
    abspath = m_rootdir + fname;
  else
    abspath = m_rootdir + "/" + fname;

  if ((length = FileUtils::length(abspath)) == (uint64_t)-1) {
    HT_ERRORF("length (stat) failed: file='%s' - %s", abspath.c_str(),
              strerror(errno));
    report_error(cb);
    return;
  }

  cb->response(length);
}


void
LocalBroker::pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset,
                   uint32_t amount) {
  OpenFileDataLocalPtr fdata;
  ssize_t nread;
  StaticBuffer buf(new uint8_t [amount], amount);

  HT_DEBUGF("pread fd=%d offset=%llu amount=%d", fd, (Llu)offset, amount);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((nread = FileUtils::pread(fdata->fd, buf.base, amount, (off_t)offset))
      == -1) {
    HT_ERRORF("pread failed: fd=%d amount=%d offset=%llu - %s", fdata->fd,
              amount, (Llu)offset, strerror(errno));
    report_error(cb);
    return;
  }

  buf.size = nread;

  cb->response(offset, buf);
}


void LocalBroker::mkdirs(ResponseCallback *cb, const char *dname) {
  String absdir;

  HT_DEBUGF("mkdirs dir='%s'", dname);

  if (dname[0] == '/')
    absdir = m_rootdir + dname;
  else
    absdir = m_rootdir + "/" + dname;

  if (!FileUtils::mkdirs(absdir)) {
    HT_ERRORF("mkdirs failed: dname='%s' - %s", absdir.c_str(),
              strerror(errno));
    report_error(cb);
    return;
  }

  cb->response_ok();
}


void LocalBroker::rmdir(ResponseCallback *cb, const char *dname) {
  String absdir;
  String cmd_str;

  if (m_verbose) {
    HT_DEBUGF("rmdir dir='%s'", dname);
  }

  if (dname[0] == '/')
    absdir = m_rootdir + dname;
  else
    absdir = m_rootdir + "/" + dname;

  if (FileUtils::exists(absdir)) {
    cmd_str = (String)"/bin/rm -rf " + absdir;
    if (system(cmd_str.c_str()) != 0) {
      HT_ERRORF("%s failed.", cmd_str.c_str());
      cb->error(Error::DFSBROKER_IO_ERROR, cmd_str);
      return;
    }
  }

#if 0
  if (rmdir(absdir.c_str()) != 0) {
    HT_ERRORF("rmdir failed: dname='%s' - %s", absdir.c_str(), strerror(errno));
    report_error(cb);
    return;
  }
#endif

  cb->response_ok();
}

void LocalBroker::readdir(ResponseCallbackReaddir *cb, const char *dname) {
  std::vector<String> listing;
  String absdir;

  HT_DEBUGF("Readdir dir='%s'", dname);

  if (dname[0] == '/')
    absdir = m_rootdir + dname;
  else
    absdir = m_rootdir + "/" + dname;

  DIR *dirp = opendir(absdir.c_str());
  if (dirp == 0) {
    HT_ERRORF("opendir('%s') failed - %s", absdir.c_str(), strerror(errno));
    report_error(cb);
    return;
  }

  struct dirent dent;
  struct dirent *dp;

  if (readdir_r(dirp, &dent, &dp) != 0) {
    HT_ERRORF("readdir('%s') failed - %s", absdir.c_str(), strerror(errno));
    (void)closedir(dirp);
    report_error(cb);
    return;
  }

  while (dp != 0) {
    if (dp->d_name[0] != '.' && dp->d_name[0] != 0)
      listing.push_back((String)dp->d_name);

    if (readdir_r(dirp, &dent, &dp) != 0) {
      HT_ERRORF("readdir('%s') failed - %s", absdir.c_str(), strerror(errno));
      report_error(cb);
      return;
    }
  }
  (void)closedir(dirp);

  HT_DEBUGF("Sending back %d listings", (int)listing.size());

  cb->response(listing);
}


void LocalBroker::flush(ResponseCallback *cb, uint32_t fd) {
  OpenFileDataLocalPtr fdata;

  HT_DEBUGF("flush fd=%d", fd);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if (fsync(fdata->fd) != 0) {
    HT_ERRORF("flush failed: fd=%d - %s", fdata->fd, strerror(errno));
    report_error(cb);
    return;
  }

  cb->response_ok();
}


void LocalBroker::status(ResponseCallback *cb) {
  cb->response_ok();
}


void LocalBroker::shutdown(ResponseCallback *cb) {
  m_open_file_map.remove_all();
  cb->response_ok();
  poll(0, 0, 2000);
}


void LocalBroker::exists(ResponseCallbackExists *cb, const char *fname) {
  String abspath;

  HT_DEBUGF("exists file='%s'", fname);

  if (fname[0] == '/')
    abspath = m_rootdir + fname;
  else
    abspath = m_rootdir + "/" + fname;

  cb->response(FileUtils::exists(abspath));
}


void
LocalBroker::rename(ResponseCallback *cb, const char *src, const char *dst) {
  String asrc =
    format("%s%s%s", m_rootdir.c_str(), *src == '/' ? "" : "/", src);
  String adst =
    format("%s%s%s", m_rootdir.c_str(), *dst == '/' ? "" : "/", dst);

  HT_DEBUGF("rename %s -> %s", asrc.c_str(), adst.c_str());

  if (std::rename(asrc.c_str(), adst.c_str()) != 0) {
    report_error(cb);
    return;
  }
  cb->response_ok();
}

void
LocalBroker::debug(ResponseCallback *cb, int32_t command,
                   StaticBuffer &serialized_parameters) {
  HT_ERRORF("debug command %d not implemented.", command);
  cb->error(Error::NOT_IMPLEMENTED, format("Unsupported debug command - %d",
            command));
}



void LocalBroker::report_error(ResponseCallback *cb) {
  char errbuf[128];
  errbuf[0] = 0;

  strerror_r(errno, errbuf, 128);

  if (errno == ENOTDIR || errno == ENAMETOOLONG || errno == ENOENT)
    cb->error(Error::DFSBROKER_BAD_FILENAME, errbuf);
  else if (errno == EACCES || errno == EPERM)
    cb->error(Error::DFSBROKER_PERMISSION_DENIED, errbuf);
  else if (errno == EBADF)
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
  else if (errno == EINVAL)
    cb->error(Error::DFSBROKER_INVALID_ARGUMENT, errbuf);
  else
    cb->error(Error::DFSBROKER_IO_ERROR, errbuf);
}
