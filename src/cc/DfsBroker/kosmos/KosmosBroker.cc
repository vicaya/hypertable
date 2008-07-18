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

#include "Common/FileUtils.h"
#include "Common/System.h"

#include "KosmosBroker.h"

using namespace Hypertable;
using namespace KFS;

KosmosBroker::KosmosBroker(PropertiesPtr &props) : m_verbose(false), m_no_flush(false) {

  const char *meta_name;
  int meta_port;

  m_verbose = props->get_bool("Hypertable.Verbose", false);

  meta_name = props->get("Kfs.MetaServer.Name", "");
  meta_port = props->get_int("Kfs.MetaServer.Port", -1);

  m_no_flush = props->get_bool("Kfs.Broker.NoFlush");

  std::cerr << "Server: " << meta_name << " " << meta_port << std::endl;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient(meta_name, meta_port);

  KFS::getKfsClientFactory()->SetDefaultClient(clnt);

  m_root_dir = "";
}



KosmosBroker::~KosmosBroker() {
}


/**
 * open
 */
void KosmosBroker::open(ResponseCallbackOpen *cb, const char *fname, uint32_t bufsz) {
  int fd;
  String abspath;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();

  if (m_verbose) {
    HT_INFOF("open file='%s' bufsz=%d", fname, bufsz);
  }

  if (fname[0] == '/')
    abspath = fname;
  else
    abspath = m_root_dir + "/" + fname;

  std::cerr << "Open file: " << abspath << std::endl;

  //fd = atomic_inc_return(&ms_uniq_id);

  /**
   * Open the file
   */
  if ((fd = clnt->Open(abspath.c_str(), O_RDONLY)) < 0) {
    string errmsg = KFS::ErrorCodeToStr(fd);
    HT_ERRORF("open failed: file='%s' - %s", abspath.c_str(), errmsg.c_str());
    ReportError(cb, fd);
    return;
  }

  {
    struct sockaddr_in addr;
    OpenFileDataKosmosPtr fdata(new OpenFileDataKosmos(fd, O_RDONLY));

    cb->get_address(addr);

    std::cerr << "Got fd for file: " << abspath << std::endl;

    m_open_file_map.create(fd, addr, fdata);

    std::cerr << "Inserted fd into open file map for file: "<< abspath << std::endl;

    cb->response(fd);
  }
}


/**
 * create
 */
void KosmosBroker::create(ResponseCallbackOpen *cb, const char *fname, bool overwrite,
            uint32_t bufsz, uint16_t replication, uint64_t blksz) {
  int fd;
  int flags;
  String abspath;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();

  if (m_verbose) {
    HT_INFOF("create file='%s' overwrite=%d bufsz=%d replication=%d blksz=%d",
                fname, (int)overwrite, bufsz, replication, blksz);
  }

  if (fname[0] == '/')
    abspath = fname;
  else
    abspath = m_root_dir + "/" + fname;


  //fd = atomic_inc_return(&ms_uniq_id);

  if (overwrite)
    flags = O_WRONLY | O_CREAT | O_TRUNC;
  else
    flags = O_WRONLY | O_CREAT | O_APPEND;

  std::cerr << "Create file: " << abspath << " " << flags << std::endl;

  /**
   * Open the file
   */
  if ((fd = clnt->Open(abspath.c_str(), flags)) < 0) {
    string errmsg = KFS::ErrorCodeToStr(fd);
    std::cerr << "Create failed: " << errmsg << std::endl;

    HT_ERRORF("open failed: file='%s' - %s", abspath.c_str(), errmsg.c_str());
    ReportError(cb, fd);
    return;
  }

  {
    struct sockaddr_in addr;
    OpenFileDataKosmosPtr fdata(new OpenFileDataKosmos(fd, O_WRONLY));

    cb->get_address(addr);

    m_open_file_map.create(fd, addr, fdata);

    cb->response(fd);
  }
}


/**
 * close
 */
void KosmosBroker::close(ResponseCallback *cb, uint32_t fd) {
  if (m_verbose) {
    HT_INFOF("close fd=%d", fd);
  }

  m_open_file_map.remove(fd);

  cb->response_ok();
}


/**
 * read
 */
void KosmosBroker::read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount) {
  OpenFileDataKosmosPtr fdata;
  ssize_t nread;
  uint64_t offset;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  StaticBuffer buf(new uint8_t [amount], amount);

  if (m_verbose) {
    HT_INFOF("read fd=%d amount=%d", fd, amount);
  }

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  offset = clnt->Tell(fdata->fd);

  if ((nread = clnt->Read(fdata->fd, (char *) buf.base, (size_t) amount)) < 0) {
    string errmsg = KFS::ErrorCodeToStr(nread);
    HT_ERRORF("read failed: fd=%d amount=%d - %s", fdata->fd, amount, errmsg.c_str());
    ReportError(cb, nread);
    return;
  }

  buf.size = nread;

  cb->response(offset, buf);

}


/**
 * append
 */
void KosmosBroker::append(ResponseCallbackAppend *cb, uint32_t fd,
                          uint32_t amount, const void *data, bool sync) {
  OpenFileDataKosmosPtr fdata;
  ssize_t nwritten;
  uint64_t offset;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  int res;

  if (m_verbose) {
    HT_INFOF("append fd=%d amount=%d", fd, amount);
  }

  if (m_no_flush)
    sync = false;

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  offset = clnt->Tell(fdata->fd);

  if ((nwritten = clnt->Write(fdata->fd, (const char *) data, (size_t) amount)) < 0) {
    string errmsg = KFS::ErrorCodeToStr(nwritten);
    HT_ERRORF("write failed: fd=%d amount=%d - %s", fdata->fd, amount, errmsg.c_str());
    ReportError(cb, nwritten);
    return;
  }

  if (sync && (res = clnt->Sync(fdata->fd)) < 0) {
    string errmsg = KFS::ErrorCodeToStr(res);
    HT_ERRORF("flush failed: fd=%d - %s", fdata->fd, errmsg.c_str());
    ReportError(cb, res);
    return;
  }

  cb->response(offset, nwritten);

  return;
}


/**
 * seek
 */
void KosmosBroker::seek(ResponseCallback *cb, uint32_t fd, uint64_t offset) {
  OpenFileDataKosmosPtr fdata;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((offset = (uint64_t) clnt->Seek(fdata->fd, offset, SEEK_SET)) == (uint64_t) -1) {
    string errmsg = KFS::ErrorCodeToStr(offset);
    HT_ERRORF("lseek failed: fd=%d offset=%lld - %s", fdata->fd, offset, errmsg.c_str());
    ReportError(cb, (int) offset);
    return;
  }

  cb->response_ok();
}


/**
 * remove
 */
void KosmosBroker::remove(ResponseCallback *cb, const char *fname) {
  String abspath;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  int res;

  if (m_verbose) {
    HT_INFOF("remove file='%s'", fname);
  }

  if (fname[0] == '/')
    abspath = fname;
  else
    abspath = m_root_dir + "/" + fname;

  if ((res = clnt->Remove(abspath.c_str())) < 0) {
    string errmsg = KFS::ErrorCodeToStr(res);
    HT_ERRORF("unlink failed: file='%s' - %s", abspath.c_str(), errmsg.c_str());
    ReportError(cb, res);
    return;
  }

  cb->response_ok();
}


/**
 * length
 */
void KosmosBroker::length(ResponseCallbackLength *cb, const char *fname) {
  String abspath;
  uint64_t length;
  int res;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  struct stat statbuf;

  if (m_verbose) {
    HT_INFOF("length file='%s'", fname);
  }

  if (fname[0] == '/')
    abspath = fname;
  else
    abspath = m_root_dir + "/" + fname;

  if ((res = clnt->Stat(abspath.c_str(), statbuf)) < 0) {
    string errmsg = KFS::ErrorCodeToStr(res);
    HT_ERRORF("length (stat) failed: file='%s' - %s", abspath.c_str(), errmsg.c_str());
    ReportError(cb, res);
    return;
  }
  length = statbuf.st_size;
  cb->response(length);
}


/**
 * pread
 */
void KosmosBroker::pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset, uint32_t amount) {
  OpenFileDataKosmosPtr fdata;
  ssize_t nread;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  StaticBuffer buf(new uint8_t [amount], amount);

  if (m_verbose) {
    HT_INFOF("pread fd=%d offset=%lld amount=%d", fd, offset, amount);
  }

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((offset = (uint64_t) clnt->Seek(fdata->fd, offset, SEEK_SET)) == (uint64_t) -1) {
    string errmsg = KFS::ErrorCodeToStr(offset);
    HT_ERRORF("lseek failed: fd=%d offset=%lld - %s", fdata->fd, offset, errmsg.c_str());
    ReportError(cb, (int) offset);
    return;
  }

  if ((nread = clnt->Read(fdata->fd, (char *) buf.base, (size_t) amount)) < 0) {
    string errmsg = KFS::ErrorCodeToStr(nread);
    HT_ERRORF("read failed: fd=%d amount=%d - %s", fdata->fd, amount, errmsg.c_str());
    ReportError(cb, nread);
    return;
  }

  buf.size = nread;

  cb->response(offset, buf);
}


/**
 * mkdirs
 */
void KosmosBroker::mkdirs(ResponseCallback *cb, const char *dname) {
  String absdir;
  String::size_type curr = 0;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  int res = 0;
  struct stat statbuf;

  if (m_verbose) {
    HT_INFOF("mkdirs dir='%s'", dname);
  }

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

    std::cerr << "Stat'ing: " << path << std::endl;

    if (clnt->Stat(path.c_str(), statbuf) == 0) {
      if (!S_ISDIR(statbuf.st_mode)) {
        res = -ENOTDIR;
        break;
      }
      continue;
    }
    std::cerr << "Mkdir'ing: " << path << std::endl;

    res = clnt->Mkdir(path.c_str());
    if (res < 0)
      break;
  }

  if (res < 0) {
    string errmsg = KFS::ErrorCodeToStr(res);
    HT_ERRORF("mkdirs failed: dname='%s' - %s", absdir.c_str(), errmsg.c_str());
    ReportError(cb, res);
    return;
  }

  cb->response_ok();
}


/**
 * rmdir
 */
void KosmosBroker::rmdir(ResponseCallback *cb, const char *dname) {
  String absdir;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  int res;

  if (m_verbose) {
    HT_INFOF("rmdir dir='%s'", dname);
  }

  if (dname[0] == '/')
    absdir = dname;
  else
    absdir = m_root_dir + "/" + dname;

  if ((res = clnt->Rmdirs(absdir.c_str())) != 0) {
    string errmsg = KFS::ErrorCodeToStr(res);
    HT_ERRORF("rmdir failed: dname='%s' - %s", absdir.c_str(), errmsg.c_str());
    ReportError(cb, res);
    return;
  }

  cb->response_ok();
}


/**
 * flush
 */
void KosmosBroker::flush(ResponseCallback *cb, uint32_t fd) {
  OpenFileDataKosmosPtr fdata;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();
  int res;

  if (m_verbose) {
    HT_INFOF("flush fd=%d", fd);
  }

  if (!m_no_flush) {
    if (!m_open_file_map.get(fd, fdata)) {
      char errbuf[32];
      sprintf(errbuf, "%d", fd);
      cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
      return;
    }

    if ((res = clnt->Sync(fdata->fd)) < 0) {
      string errmsg = KFS::ErrorCodeToStr(res);
      HT_ERRORF("flush failed: fd=%d - %s", fdata->fd, errmsg.c_str());
      ReportError(cb, res);
      return;
    }
  }

  cb->response_ok();
}


/**
 * status
 */
void KosmosBroker::status(ResponseCallback *cb) {
  cb->response_ok();
}


/**
 * shutdown
 */
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

  if (m_verbose) {
    HT_INFOF("readdir dir_name='%s'", dname);
  }

  if (dname[0] == '/')
    abs_path = dname;
  else
    abs_path = m_root_dir + "/" + dname;

  if ((error = clnt->Readdir(abs_path.c_str(), listing)) < 0) {
    string errmsg = KFS::ErrorCodeToStr(error);
    HT_ERRORF("readdir failed: file='%s' - %s", abs_path.c_str(), errmsg.c_str());
    ReportError(cb, error);
    return;
  }

  for (size_t i=0;i<listing.size(); i++) {
         if (listing[i] == "." || listing[i] == "..")
           continue;
         stripped_listing.push_back(listing[i]);
       }

  HT_INFOF("Sending back %d listings", stripped_listing.size());

  cb->response(stripped_listing);
}


void KosmosBroker::exists(ResponseCallbackExists *cb, const char *fname) {
  String abspath;
  KfsClientPtr clnt = KFS::getKfsClientFactory()->GetClient();

  if (m_verbose) {
    HT_INFOF("exists file='%s'", fname);
  }

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

  if (m_verbose)
    HT_INFOF("rename %s -> %s", src, dst);

  String abs_src =
    format("%s%s%s", m_root_dir.c_str(), *src == '/' ? "" : "/", src);
  String abs_dst =
    format("%s%s%s", m_root_dir.c_str(), *dst == '/' ? "" : "/", dst);

  if ((error = clnt->Rename(src, dst)) < 0)
    ReportError(cb, error);
  else
    cb->response_ok();
}


/**
 * ReportError
 */
void KosmosBroker::ReportError(ResponseCallback *cb, int error) {

  string errbuf = KFS::ErrorCodeToStr(error);

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
