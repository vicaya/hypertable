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


#ifndef HYPERTABLE_FDUTILS_H
#define HYPERTABLE_FDUTILS_H

extern "C" {
#include <fcntl.h>
#include <sys/types.h>
#include <sys/uio.h>
}

namespace hypertable {

  class FileUtils {

  public:
    static ssize_t Read(int fd, void *vptr, size_t n);
    static ssize_t Pread(int fd, void *vptr, size_t n, off_t offset);
    static ssize_t Write(int fd, const void *vptr, size_t n);
    static ssize_t Writev(int fd, const struct iovec *vector, int count);
    static void SetFlags(int fd, int flags);
    static char *FileToBuffer(const char *fname, off_t *lenp);
    static bool Mkdirs(const char *dirname);
  };

}

#endif // HYPERTABLE_FDUITLS_H

