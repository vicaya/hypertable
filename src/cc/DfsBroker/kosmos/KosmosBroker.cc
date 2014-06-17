/**
 * Copyright (C) 2008 Sriram Rao (Kosmix Corp)
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

extern "C" {
#include <fcntl.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <time.h>
}

#include "Common/Filesystem.h"
#include "Common/FileUtils.h"
#include "Common/System.h"

#include "KosmosBroker.h"

using namespace Hypertable;
using namespace KFS;

atomic_t KosmosBroker::ms_next_fd = ATOMIC_INIT(0);


KosmosBroker::KosmosBroker(PropertiesPtr &cfg) {
  m_verbose = cfg->get_bool("Hypertable.Verbose");

  String meta_name = cfg->get_str("Kfs.MetaServer.Name");
  int meta_port = cfg->get_i16("Kfs.MetaServer.Port");

  KfsClientPtr kfsclient = KFS::getKfsClientFactory()->GetClient(meta_name,
                                                                 meta_port);
  KFS::getKfsClientFactory()->SetDefaultClient(kfsclient);
}


KosmosBroker::~KosmosBroker() {
}


void
KosmosBroker::open(ResponseCallbackOpen *cb, const char *fname,
                   uint32_t flags, uint32_t bufsz) {
  int fd, local_fd;
  String abspath;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();

  HT_DEBUGF("open file='%s' bufsz=%d", fname, bufsz);

  if (fname[0] == '/')
    abspath = fname;
  else
    abspath = m_root_dir + "/" + fname;

  HT_DEBUG_OUT << "Open file: " << abspath << HT_END;

  fd = atomic_inc_return(&ms_next_fd);

  if ((local_fd = clnt->Open(abspath.c_str(), O_RDONLY)) < 0) {
    string errmsg = KFS::ErrorCodeToStr(fd);
    HT_ERRORF("open failed: file='%s' - %s", abspath.c_str(), errmsg.c_str());
    report_error(cb, local_fd);
    return;
  }

  HT_INFOF("open( %s ) fd=%d local_fd=%d", fname, fd, local_fd);

  {
    struct sockaddr_in addr;
    OpenFileDataKosmosPtr fdata(new OpenFileDataKosmos(local_fd, O_RDONLY));

    cb->get_address(addr);

    m_open_file_map.create(fd, addr, fdata);

    cb->response(fd);
  }
}


void
KosmosBroker::create(ResponseCallbackOpen *cb, const char *fname,
		     uint32_t flags, int32_t bufsz, int16_t replication, int64_t blksz) {
  int fd, local_fd;
  int oflags;
  String abspath;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();

  HT_DEBUGF("create file='%s' flags=%u bufsz=%d replication=%d blksz=%lld",
            fname, flags, (int)bufsz, (int)replication, (Lld)blksz);

  if (fname[0] == '/')
    abspath = fname;
  else
    abspath = m_root_dir + "/" + fname;

  fd = atomic_inc_return(&ms_next_fd);

  if (flags & Filesystem::OPEN_FLAG_OVERWRITE)
    oflags = O_WRONLY | O_CREAT | O_TRUNC;
  else
    oflags = O_WRONLY | O_CREAT | O_APPEND;

  if ((local_fd = clnt->Open(abspath.c_str(), oflags)) < 0) {
    HT_ERROR_OUT <<"KfsClient::Open failed: file='"<< abspath
        << "' - "<< KFS::ErrorCodeToStr(fd) << HT_END;
    report_error(cb, local_fd);
    return;
  }

  HT_INFOF("create( %s ) fd=%d local_fd=%d", fname, fd, local_fd);

  {
    struct sockaddr_in addr;
    OpenFileDataKosmosPtr fdata(new OpenFileDataKosmos(local_fd, O_WRONLY));

    cb->get_address(addr);

    m_open_file_map.create(fd, addr, fdata);

    cb->response(fd);
  }
}


void KosmosBroker::close(ResponseCallback *cb, uint32_t fd) {
  if (m_verbose) {
    HT_INFOF("close fd=%d", fd);
  }

  m_open_file_map.remove(fd);

  cb->response_ok();
}


void
KosmosBroker::read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount) {
  OpenFileDataKosmosPtr fdata;
  ssize_t nread;
  uint64_t offset;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  StaticBuffer buf(new uint8_t [amount], amount);

  HT_DEBUGF("read fd=%d amount=%d", fd, amount);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  offset = clnt->Tell(fdata->fd);

  if ((nread = clnt->Read(fdata->fd, (char *) buf.base, (size_t) amount)) < 0) {
    string errmsg = KFS::ErrorCodeToStr(nread);
    HT_ERRORF("read failed: fd=%d amount=%d - %s", fdata->fd, amount,
              errmsg.c_str());
    report_error(cb, nread);
    return;
  }

  buf.size = nread;

  cb->response(offset, buf);

}


void
KosmosBroker::append(ResponseCallbackAppend *cb, uint32_t fd, uint32_t amount,
                     const void *data, bool sync) {
  OpenFileDataKosmosPtr fdata;
  ssize_t nwritten;
  uint64_t offset;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  int res;

  HT_DEBUGF("append fd=%d amount=%d", fd, amount);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  offset = clnt->Tell(fdata->fd);

  if ((nwritten = clnt->Write(fdata->fd, (const char *) data, (size_t) amount))
      < 0) {
    string errmsg = KFS::ErrorCodeToStr(nwritten);
    HT_ERRORF("write failed: fd=%d amount=%d - %s", fdata->fd, amount,
              errmsg.c_str());
    report_error(cb, nwritten);
    return;
  }

  if (sync && (res = clnt->Sync(fdata->fd)) < 0) {
    string errmsg = KFS::ErrorCodeToStr(res);
    HT_ERRORF("flush failed: fd=%d - %s", fdata->fd, errmsg.c_str());
    report_error(cb, res);
    return;
  }

  cb->response(offset, nwritten);

  return;
}


void KosmosBroker::seek(ResponseCallback *cb, uint32_t fd, uint64_t offset) {
  OpenFileDataKosmosPtr fdata;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();

  HT_DEBUGF("seek fd=%lu offset=%llu", (Lu)fd, (Llu)offset);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((offset = (uint64_t) clnt->Seek(fdata->fd, offset, SEEK_SET))
      == (uint64_t) -1) {
    string errmsg = KFS::ErrorCodeToStr(offset);
    HT_ERRORF("lseek failed: fd=%d offset=%lld - %s", fdata->fd, (Llu)offset,
              errmsg.c_str());
    report_error(cb, (int) offset);
    return;
  }

  cb->response_ok();
}


void KosmosBroker::remove(ResponseCallback *cb, const char *fname) {
  String abspath;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  int res;

  HT_DEBUGF("remove file='%s'", fname);

  if (fname[0] == '/')
    abspath = fname;
  else
    abspath = m_root_dir + "/" + fname;

  if ((res = clnt->Remove(abspath.c_str())) < 0) {
    string errmsg = KFS::ErrorCodeToStr(res);
    HT_ERRORF("unlink failed: file='%s' - %s", abspath.c_str(), errmsg.c_str());
    report_error(cb, res);
    return;
  }

  cb->response_ok();
}


void KosmosBroker::length(ResponseCallbackLength *cb, const char *fname) {
  String abspath;
  uint64_t length;
  int res;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  struct stat statbuf;

   HT_DEBUGF("length file='%s'", fname);

  if (fname[0] == '/')
    abspath = fname;
  else
    abspath = m_root_dir + "/" + fname;

  if ((res = clnt->Stat(abspath.c_str(), statbuf)) < 0) {
    string errmsg = KFS::ErrorCodeToStr(res);
    HT_ERRORF("length (stat) failed: file='%s' - %s", abspath.c_str(),
              errmsg.c_str());
    report_error(cb, res);
    return;
  }
  length = statbuf.st_size;
  cb->response(length);
}


void
KosmosBroker::pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset,
                    uint32_t amount) {
  OpenFileDataKosmosPtr fdata;
  ssize_t nread;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  StaticBuffer buf(new uint8_t [amount], amount);

  HT_DEBUGF("pread fd=%d offset=%lld amount=%d", fd, (Lld)offset, amount);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  // remember the last offset before pread()
  uint64_t offset_last = clnt->Tell(fdata->fd);

  if ((offset = (uint64_t) clnt->Seek(fdata->fd, offset, SEEK_SET))
      == (uint64_t) -1) {
    string errmsg = KFS::ErrorCodeToStr(offset);
    HT_ERRORF("lseek failed: fd=%d offset=%lld - %s", fdata->fd, (Lld)offset,
              errmsg.c_str());
    report_error(cb, (int) offset);
    return;
  }

  if ((nread = clnt->Read(fdata->fd, (char *) buf.base, (size_t) amount)) < 0) {
    string errmsg = KFS::ErrorCodeToStr(nread);
    HT_ERRORF("read failed: fd=%d amount=%d - %s", fdata->fd, amount,
              errmsg.c_str());
    report_error(cb, nread);
    return;
  }

  // restore the last offset when pread() done
  if ((offset = (uint64_t) clnt->Seek(fdata->fd, offset_last, SEEK_SET)) == (uint64_t) -1) {
    string errmsg = KFS::ErrorCodeToStr(offset_last);
    HT_ERRORF("lseek failed: fd=%d offset=%lld - %s", fdata->fd,
              (Lld)offset_last, errmsg.c_str());
    report_error(cb, (int)offset);
    return;
  }

  buf.size = nread;

  cb->response(offset, buf);
}


void KosmosBroker::mkdirs(ResponseCallback *cb, const char *dname) {
  String absdir;
  String::size_type curr = 0;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  int res = 0;
  struct stat statbuf;

  HT_DEBUGF("mkdirs dir='%s'", dname);

  if (dname[0] == '/')
    absdir = dname;
  else
    absdir = m_root_dir + "/" + dname;

  String path;
  while (curr != String::npos) {
    curr = absdir.find('/', curr + 1);
    if (curr != String::npos)
      path.assign(absdir, 0, curr);
    else
      path = absdir;

    if (clnt->Stat(path.c_str(), statbuf) == 0) {
      if (!S_ISDIR(statbuf.st_mode)) {
        res = -ENOTDIR;
        break;
      }
      continue;
    }

    res = clnt->Mkdir(path.c_str());
    if (res < 0)
      break;
  }

  if (res < 0) {
    string errmsg = KFS::ErrorCodeToStr(res);
    HT_ERRORF("mkdirs failed: dname='%s' - %s", absdir.c_str(), errmsg.c_str());
    report_error(cb, res);
    return;
  }

  cb->response_ok();
}


void KosmosBroker::rmdir(ResponseCallback *cb, const char *dname) {
  String absdir;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  int res;

  HT_DEBUGF("rmdir dir='%s'", dname);

  if (dname[0] == '/')
    absdir = dname;
  else
    absdir = m_root_dir + "/" + dname;

  if ((res = clnt->Rmdirs(absdir.c_str())) != 0) {
    if (res != -ENOENT && res != -ENOTDIR) {
      string errmsg = KFS::ErrorCodeToStr(res);
      HT_ERRORF("rmdir failed: dname='%s' - %s", absdir.c_str(),
                errmsg.c_str());
      report_error(cb, res);
      return;
    }
  }

  cb->response_ok();
}


void KosmosBroker::flush(ResponseCallback *cb, uint32_t fd) {
  OpenFileDataKosmosPtr fdata;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  int res;

  HT_DEBUGF("flush fd=%d", fd);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((res = clnt->Sync(fdata->fd)) < 0) {
    string errmsg = KFS::ErrorCodeToStr(res);
    HT_ERRORF("flush failed: fd=%d - %s", fdata->fd, errmsg.c_str());
    report_error(cb, res);
    return;
  }

  cb->response_ok();
}


void KosmosBroker::status(ResponseCallback *cb) {
  cb->response_ok();
}


void KosmosBroker::shutdown(ResponseCallback *cb) {
  m_open_file_map.remove_all();
  cb->response_ok();
  poll(0, 0, 2000);
}


void KosmosBroker::readdir(ResponseCallbackReaddir *cb, const char *dname) {
  std::vector<String> listing;
  std::vector<String> stripped_listing;
  String abs_path;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  int error;

  HT_DEBUGF("readdir dir_name='%s'", dname);

  if (dname[0] == '/')
    abs_path = dname;
  else
    abs_path = m_root_dir + "/" + dname;

  if ((error = clnt->Readdir(abs_path.c_str(), listing)) < 0) {
    string errmsg = KFS::ErrorCodeToStr(error);
    HT_ERRORF("readdir failed: file='%s' - %s", abs_path.c_str(),
              errmsg.c_str());
    report_error(cb, error);
    return;
  }

  for (size_t i=0;i<listing.size(); i++) {
         if (listing[i] == "." || listing[i] == "..")
           continue;
         stripped_listing.push_back(listing[i]);
       }

  HT_INFOF("Sending back %d listings", (int)stripped_listing.size());

  cb->response(stripped_listing);
}


void KosmosBroker::exists(ResponseCallbackExists *cb, const char *fname) {
  String abspath;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();

  HT_DEBUGF("exists file='%s'", fname);

  if (fname[0] == '/')
    abspath = fname;
  else
    abspath = m_root_dir + "/" + fname;

  cb->response(clnt->Exists(abspath.c_str()));
}


void
KosmosBroker::rename(ResponseCallback *cb, const char *src, const char *dst) {
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  int error;

  HT_DEBUGF("rename %s -> %s", src, dst);

  String abs_src =
    format("%s%s%s", m_root_dir.c_str(), *src == '/' ? "" : "/", src);
  String abs_dst =
    format("%s%s%s", m_root_dir.c_str(), *dst == '/' ? "" : "/", dst);

  if ((error = clnt->Rename(src, dst)) < 0)
    report_error(cb, error);
  else
    cb->response_ok();
}


void
KosmosBroker::debug(ResponseCallback *cb, int32_t command,
                    StaticBuffer &serialized_parameters) {
  HT_ERRORF("debug command %d not implemented.", command);
  cb->error(Error::NOT_IMPLEMENTED, format("Unsupported debug command - %d",
            command));
}


void KosmosBroker::report_error(ResponseCallback *cb, int error) {

  string errbuf = KFS::ErrorCodeToStr(error);

  error *= -1;

  if (error == ENOTDIR || error == ENAMETOOLONG || error == ENOENT)
    cb->error(Error::DFSBROKER_BAD_FILENAME, errbuf.c_str());
  else if (error == EACCES || error == EPERM)
    cb->error(Error::DFSBROKER_PERMISSION_DENIED, errbuf.c_str());
  else if (error == EBADF)
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf.c_str());
  else if (error == EINVAL)
    cb->error(Error::DFSBROKER_INVALID_ARGUMENT, errbuf.c_str());
  else
    cb->error(Error::DFSBROKER_IO_ERROR, errbuf.c_str());
}
