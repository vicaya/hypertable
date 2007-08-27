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
#include <iomanip>
#include <iostream>
#include <sstream>

extern "C" {
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
}

#include <boost/shared_array.hpp>

#include "FileUtils.h"
#include "Logger.h"

using namespace hypertable;
using namespace std;

/**
 */
ssize_t FileUtils::Read(int fd, void *vptr, size_t n) {
  size_t nleft;
  ssize_t nread;
  char *ptr;

  ptr = (char *)vptr;
  nleft = n;
  while (nleft > 0) {
    if ( (nread = read(fd, ptr, nleft)) < 0) {
      if (errno == EINTR)
	nread = 0;/* and call read() again */
      else if (errno == EAGAIN)
	break;
      else
	return -1;
    } else if (nread == 0)
      break;/* EOF */

    nleft -= nread;
    ptr   += nread;
  }
  return n - nleft;
}

/**
 */
ssize_t FileUtils::Pread(int fd, void *vptr, size_t n, off_t offset) {
  size_t nleft;
  ssize_t nread;
  char *ptr;

  ptr = (char *)vptr;
  nleft = n;
  while (nleft > 0) {
    if ( (nread = pread(fd, ptr, nleft, offset)) < 0) {
      if (errno == EINTR)
	nread = 0;/* and call read() again */
      else if (errno == EAGAIN)
	break;
      else
	return -1;
    } else if (nread == 0)
      break;/* EOF */

    nleft -= nread;
    ptr   += nread;
    offset += nread;
  }
  return n - nleft;
}

/**
 */
ssize_t FileUtils::Write(int fd, const void *vptr, size_t n) {
  size_t nleft;
  ssize_t nwritten;
  const char *ptr;

  ptr = (const char *)vptr;
  nleft = n;
  while (nleft > 0) {
    if ((nwritten = write(fd, ptr, nleft)) <= 0) {
      if (errno == EINTR)
	nwritten = 0; /* and call write() again */
      if (errno == EAGAIN)
	break;
      else
	return -1; /* error */
    }

    nleft -= nwritten;
    ptr   += nwritten;
  }
  return n - nleft;
}

ssize_t FileUtils::Writev(int fd, const struct iovec *vector, int count) {
  ssize_t nwritten;
  while ((nwritten = writev(fd, vector, count)) <= 0) {
    if (errno == EINTR)
      nwritten = 0; /* and call write() again */
    if (errno == EAGAIN) {
      nwritten = 0;
      break;
    }
    else
      return -1; /* error */
  }
  return nwritten;
}


/* flags are file status flags to turn on */
void FileUtils::SetFlags(int fd, int flags) {
  int val;

  if ( (val = fcntl(fd, F_GETFL, 0)) < 0)
    cerr << "fcnt(F_GETFL) failed : " << strerror(errno) << endl;

  val |= flags;

  if (fcntl(fd, F_SETFL, val) < 0)
    cerr << "fcnt(F_SETFL) failed : " << strerror(errno) << endl;
}



/**
 */
char *FileUtils::FileToBuffer(const char *fname, off_t *lenp) {
  struct stat statbuf;
  int fd;

  *lenp = 0;

  if ((fd = open(fname, O_RDONLY)) < 0) {
    LOG_VA_ERROR("open(\"%s\") failure - %s", fname,  strerror(errno));
    return 0;
  }

  if (fstat(fd, &statbuf) < 0) {
    LOG_VA_ERROR("fstat(\"%s\") failure - %s", fname,  strerror(errno));
    return 0;
  }

  *lenp = statbuf.st_size;

  char *rbuf = new char [ *lenp + 1 ];

  ssize_t nread = Read(fd, rbuf, *lenp);

  if (nread == (ssize_t)-1) {
    LOG_VA_ERROR("read(\"%s\") failure - %s", fname,  strerror(errno));
    delete [] rbuf;
    *lenp = 0;
    return 0;
  }

  if (nread < *lenp) {
    LOG_VA_WARN("short read (%d of %d bytes)", nread, *lenp);
    *lenp = nread;
  }

  rbuf[nread] = 0;

  return rbuf;
}


bool FileUtils::Mkdirs(const char *dirname) {
  struct stat statbuf;
  boost::shared_array<char> tmpDirPtr(new char [ strlen(dirname) + 1 ]);
  char *tmpDir = tmpDirPtr.get();
  char *ptr = tmpDir+1;

  strcpy(tmpDir, dirname);
  
  while ((ptr = strchr(ptr, '/')) != 0) {
    *ptr = 0;
    if (stat(tmpDir, &statbuf) != 0) {
      if (errno == ENOENT) {
	if (mkdir(tmpDir, 0755) != 0) {
	  LOG_VA_ERROR("Problem creating directory '%s' - %s", tmpDir, strerror(errno));
	  return false;
	}
      }
      else {
	LOG_VA_ERROR("Problem stat'ing directory '%s' - %s", tmpDir, strerror(errno));
	return false;
      }
    }
    *ptr++ = '/';
  }

  if (stat(tmpDir, &statbuf) != 0) {
    if (errno == ENOENT) {
      if (mkdir(tmpDir, 0755) != 0) {
	LOG_VA_ERROR("Problem creating directory '%s' - %s", tmpDir, strerror(errno));
	return false;
      }
    }
    else {
      LOG_VA_ERROR("Problem stat'ing directory '%s' - %s", tmpDir, strerror(errno));
      return false;
    }
  }

  return true;
}


bool FileUtils::Exists(const char *fname) {
  struct stat statbuf;
  if (stat(fname, &statbuf) != 0)
    return false;
  return true;
}


off_t FileUtils::Length(const char *fname) {
  struct stat statbuf;
  if (stat(fname, &statbuf) != 0)
    return (off_t)-1;
  return statbuf.st_size;
}
