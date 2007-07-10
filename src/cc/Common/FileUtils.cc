/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include <iomanip>
#include <iostream>
#include <sstream>
using namespace std;

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
