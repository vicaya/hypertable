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
#ifndef HYPERTABLE_FDUTILS_H
#define HYPERTABLE_FDUTILS_H

extern "C" {
#include <sys/socket.h>
#include <sys/types.h>
}
#include "Common/String.h"

namespace Hypertable {

  class FileUtils {

  public:
    static ssize_t read(const String &fname, String &contents);
    static ssize_t read(int fd, void *vptr, size_t n);
    static ssize_t pread(int fd, void *vptr, size_t n, off_t offset);
    static ssize_t write(const String &fname, String &contents);
    static ssize_t write(int fd, const void *vptr, size_t n);
    static ssize_t writev(int fd, const struct iovec *vector, int count);
    static ssize_t sendto(int fd, const void *vptr, size_t n,
                          const sockaddr *to, socklen_t tolen);
    static ssize_t send(int fd, const void *vptr, size_t n);
    static ssize_t recvfrom(int fd, void *vptr, size_t n,
                            struct sockaddr *from, socklen_t *fromlen);
    static ssize_t recv(int fd, void *vptr, size_t n);
    static void set_flags(int fd, int flags);
    static char *file_to_buffer(const String &fname, off_t *lenp);
    static bool mkdirs(const String &dirname);
    static bool exists(const String &fname);
    static bool unlink(const String &fname);
    static uint64_t size(const String &fname);
    static off_t length(const String &fname);
    static void add_trailing_slash(String &path);
    static bool expand_tilde(String &fname);
#ifdef HT_XATTR_ENABLED
    static int
    getxattr(const String &path, const String &name, void *value, size_t size);
    static int fgetxattr(int fd, const String &name, void *value, size_t size);
    static int
    setxattr(const String &path, const String &name, const void *value,
             size_t size, int flags);
    static int
    fsetxattr(int fd, const String &name, const void *value, size_t size,
              int flags);
    static int removexattr(const String &path, const String &name);
    static int fremovexattr(int fd, const String &name);
#endif
  };

}

#endif // HYPERTABLE_FDUITLS_H

