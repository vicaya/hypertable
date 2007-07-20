/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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

