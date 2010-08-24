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
#if defined(__sun__)
#include <sys/fcntl.h>
#endif
#include <sys/uio.h>
#include <unistd.h>
}

#include "AsyncComm/ReactorFactory.h"

#include "Common/String.h"
#include "Common/Filesystem.h"
#include "Common/FileUtils.h"
#include "Common/Path.h"
#include "Common/System.h"
#include "Common/SystemInfo.h"

#include "LocalBroker.h"

using namespace Hypertable;

atomic_t LocalBroker::ms_next_fd = ATOMIC_INIT(0);

LocalBroker::LocalBroker(PropertiesPtr &cfg) {
  m_verbose = cfg->get_bool("verbose");
  m_directio = cfg->get_bool("DfsBroker.Local.DirectIO");

#if defined(__linux__)
  // disable direct i/o for kernels < 2.6
  if (m_directio) {
    if (System::os_info().version_major == 2 &&
        System::os_info().version_minor < 6)
      m_directio = false;
  }
#endif

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
LocalBroker::open(ResponseCallbackOpen *cb, const char *fname, 
                  uint32_t flags, uint32_t bufsz) {
  int fd, local_fd;
  String abspath;

  HT_DEBUGF("open file='%s' flags=%u bufsz=%d", fname, flags, bufsz);

  if (fname[0] == '/')
    abspath = m_rootdir + fname;
  else
    abspath = m_rootdir + "/" + fname;

  fd = atomic_inc_return(&ms_next_fd);

  int oflags = O_RDONLY;

  if (flags & Filesystem::OPEN_FLAG_DIRECTIO) {
#ifdef O_DIRECT
    oflags |= O_DIRECT;
#endif
  }

  /**
   * Open the file
   */
  if ((local_fd = ::open(abspath.c_str(), oflags)) == -1) {
    report_error(cb);
    HT_ERRORF("open failed: file='%s' - %s", abspath.c_str(), strerror(errno));
    return;
  }

#if defined(__APPLE__)
#ifdef F_NOCACHE
  //fcntl(local_fd, F_NOCACHE, 1);
#endif  
#elif defined(__sun__)
  directio(local_fd, DIRECTIO_ON);
#endif

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
LocalBroker::create(ResponseCallbackOpen *cb, const char *fname, uint32_t flags,
                    int32_t bufsz, int16_t replication, int64_t blksz) {
  int fd, local_fd;
  int oflags = O_WRONLY | O_CREAT;
  String abspath;

  HT_DEBUGF("create file='%s' flags=%u bufsz=%d replication=%d blksz=%lld",
            fname, flags, bufsz, (int)replication, (Lld)blksz);

  if (fname[0] == '/')
    abspath = m_rootdir + fname;
  else
    abspath = m_rootdir + "/" + fname;

  fd = atomic_inc_return(&ms_next_fd);

  if (flags & Filesystem::OPEN_FLAG_OVERWRITE)
    oflags |= O_TRUNC;
  else
    oflags |= O_APPEND;

  if (flags & Filesystem::OPEN_FLAG_DIRECTIO) {
#ifdef O_DIRECT
    oflags |= O_DIRECT;
#endif
  }

  /**
   * Open the file
   */
  if ((local_fd = ::open(abspath.c_str(), oflags, 0644)) == -1) {
    report_error(cb);
    HT_ERRORF("open failed: file='%s' - %s", abspath.c_str(), strerror(errno));
    return;
  }

#if defined(__APPLE__)
#ifdef F_NOCACHE
    fcntl(local_fd, F_NOCACHE, 1);
#endif  
#elif defined(__sun__)
    directio(local_fd, DIRECTIO_ON);
#endif

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
  uint8_t *readbuf;
#if defined(__linux__)
  void *vptr = 0;
  HT_ASSERT(posix_memalign(&vptr, HT_DIRECT_IO_ALIGNMENT, amount) == 0);
  readbuf = (uint8_t *)vptr;
#else
  readbuf = new uint8_t [amount];
#endif
  
  StaticBuffer buf(readbuf, amount);

  HT_DEBUGF("read fd=%d amount=%d", fd, amount);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    HT_ERRORF("bad file handle: %d", fd);
    return;
  }

  if ((offset = (uint64_t)lseek(fdata->fd, 0, SEEK_CUR)) == (uint64_t)-1) {
    report_error(cb);
    HT_ERRORF("lseek failed: fd=%d offset=0 SEEK_CUR - %s", fdata->fd,
              strerror(errno));
    return;
  }

  if ((nread = FileUtils::read(fdata->fd, buf.base, amount)) == -1) {
    report_error(cb);
    HT_ERRORF("read failed: fd=%d offset=%llu amount=%d - %s",
	      fdata->fd, (Llu)offset, amount, strerror(errno));
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
    report_error(cb);
    HT_ERRORF("lseek failed: fd=%d offset=0 SEEK_CUR - %s", fdata->fd,
              strerror(errno));
    return;
  }

  if ((nwritten = FileUtils::write(fdata->fd, data, amount)) == -1) {
    report_error(cb);
    HT_ERRORF("write failed: fd=%d offset=%llu amount=%d data=%p- %s",
	      fdata->fd, (Llu)offset, amount, data, strerror(errno));
    return;
  }

  if (sync && fsync(fdata->fd) != 0) {
    report_error(cb);
    HT_ERRORF("flush failed: fd=%d - %s", fdata->fd, strerror(errno));
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
    report_error(cb);
    HT_ERRORF("lseek failed: fd=%d offset=%llu - %s", fdata->fd, (Llu)offset,
              strerror(errno));
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
    report_error(cb);
    HT_ERRORF("unlink failed: file='%s' - %s", abspath.c_str(),
              strerror(errno));
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
    report_error(cb);
    HT_ERRORF("length (stat) failed: file='%s' - %s", abspath.c_str(),
              strerror(errno));
    return;
  }

  cb->response(length);
}


void
LocalBroker::pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset,
                   uint32_t amount) {
  OpenFileDataLocalPtr fdata;
  ssize_t nread;
  uint8_t *readbuf;
#if defined(__linux__)
  void *vptr = 0;
  HT_ASSERT(posix_memalign(&vptr, HT_DIRECT_IO_ALIGNMENT, amount) == 0);
  readbuf = (uint8_t *)vptr;
#else
  readbuf = new uint8_t [amount];
#endif
  
  StaticBuffer buf(readbuf, amount);

  HT_DEBUGF("pread fd=%d offset=%llu amount=%d", fd, (Llu)offset, amount);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((nread = FileUtils::pread(fdata->fd, buf.base, amount, (off_t)offset))
      == -1) {
    report_error(cb);
    HT_ERRORF("pread failed: fd=%d amount=%d offset=%llu - %s", fdata->fd,
              amount, (Llu)offset, strerror(errno));
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
    report_error(cb);
    HT_ERRORF("mkdirs failed: dname='%s' - %s", absdir.c_str(),
              strerror(errno));
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
    report_error(cb);
    HT_ERRORF("rmdir failed: dname='%s' - %s", absdir.c_str(), strerror(errno));
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
    report_error(cb);
    HT_ERRORF("opendir('%s') failed - %s", absdir.c_str(), strerror(errno));
    return;
  }

  struct dirent *dp = (struct dirent *)new uint8_t [sizeof(struct dirent)+1025];
  struct dirent *result;

  if (readdir_r(dirp, dp, &result) != 0) {
    report_error(cb);
    HT_ERRORF("readdir('%s') failed - %s", absdir.c_str(), strerror(errno));
    (void)closedir(dirp);
    delete [] (uint8_t *)dp;
    return;
  }

  while (result != 0) {
    if (result->d_name[0] != '.' && result->d_name[0] != 0) {
      listing.push_back((String)result->d_name);
      //HT_INFOF("readdir Adding listing '%s'", result->d_name);
    }

    if (readdir_r(dirp, dp, &result) != 0) {
      report_error(cb);
      HT_ERRORF("readdir('%s') failed - %s", absdir.c_str(), strerror(errno));
      delete [] (uint8_t *)dp;
      return;
    }
  }
  (void)closedir(dirp);

  delete [] (uint8_t *)dp;

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
    report_error(cb);
    HT_ERRORF("flush failed: fd=%d - %s", fdata->fd, strerror(errno));
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
