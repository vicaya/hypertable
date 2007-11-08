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

#ifndef HYPERTABLE_FILESYSTEM_H
#define HYPERTABLE_FILESYSTEM_H

#include "AsyncComm/DispatchHandler.h"

namespace hypertable {

  /**
   * Abstract base class for filesystems.
   */
  class Filesystem {
  public:

    /** Opens a file asynchronously.  Issues an open file request.  The caller will
     * get notified of successful completion or error via the given dispatch handler.
     * It is up to the caller to deserialize the returned file descriptor from
     * the MESSAGE event object.
     *
     * @param name absolute path name of file to open
     * @param handler dispatch handler
     * @return Error::OK on success or error code on failure
     */
    virtual int open(std::string &name, DispatchHandler *handler) = 0;

    /** Opens a file.  Issues an open file request and waits for it to complete.
     *
     * @param name absolute path name of file to open
     * @param fdp address of variable to hold return file descriptor
     * @return Error::OK on success or error code on failure
     */
    virtual int open(std::string &name, int32_t *fdp) = 0;

    /** Opens a file in buffered (readahead) mode.  Issues an open file request and waits for it to complete.
     * Turns on readahead mode so that data is prefetched.
     *
     * @param name absolute path name of file to open
     * @param bufSize readahead buffer size
     * @param fdp address of variable to hold return file descriptor
     * @return Error::OK on success or error code on failure
     */
    virtual int open_buffered(std::string &name, uint32_t bufSize, int32_t *fdp) = 0;

    /** Creates a file asynchronously.  Issues a create file request with various create
     * mode parameters.   The caller will get notified of successful completion
     * or error via the given dispatch handler.  It is up to the caller to deserialize
     * the returned file descriptor from the MESSAGE event object.
     *
     * @param name absolute path name of file to open
     * @param overwrite overwrite the file if it exists
     * @param bufferSize buffer size to use for the underlying FS
     * @param replication replication factor to use for this file
     * @param blockSize block size to use for the underlying FS
     * @param handler dispatch handler
     * @return Error::OK on success or error code on failure
     */
    virtual int create(std::string &name, bool overwrite, int32_t bufferSize,
		       int32_t replication, int64_t blockSize, DispatchHandler *handler) = 0;

    /** Creates a file.  Issues a create file request and waits for completion
     *
     * @param name absolute path name of file to open
     * @param overwrite overwrite the file if it exists
     * @param bufferSize buffer size to use for the underlying FS
     * @param replication replication factor to use for this file
     * @param blockSize block size to use for the underlying FS
     * @param fdp address of variable to hold return file descriptor
     * @return Error::OK on success or error code on failure
     */
    virtual int create(std::string &name, bool overwrite, int32_t bufferSize,
		       int32_t replication, int64_t blockSize, int32_t *fdp) = 0;

    /** Closes a file asynchronously.  Issues a close file request.
     * The caller will get notified of successful completion or error via
     * the given dispatch handler.
     *
     * @param fd open file descriptor
     * @param handler dispatch handler
     * @return Error::OK on success or error code on failure
     */
    virtual int close(int32_t fd, DispatchHandler *handler) = 0;

    /** Closes a file.  Issues a close command and waits for it
     * to complete.
     *
     * @param fd open file descriptor
     * @return Error::OK on success or error code on failure
     */
    virtual int close(int32_t fd) = 0;

    /** Reads data from a file at the current position asynchronously.  Issues
     * a read request.  The caller will get notified of successful completion or
     * error via the given dispatch handler.  It's up to the caller to deserialize
     * the returned data in the MESSAGE event object.  EOF is indicated by a short read.
     *
     * @param fd open file descriptor
     * @param amount amount of data to read
     * @param handler dispatch handler
     * @return Error::OK on success or error code on failure
     */
    virtual int read(int32_t fd, uint32_t amount, DispatchHandler *handler) = 0;

    /** Reads data from a file at the current position.  Issues a read
     * request and waits for it to complete, returning the read data.
     * EOF is indicated by a short read.
     *
     * @param fd open file descriptor
     * @param amount amount of data to read
     * @param dst destination buffer for read data
     * @param nreadp address of variable to hold amount of data read.
     * @return Error::OK on success or error code on failure
     */
    virtual int read(int32_t fd, uint32_t amount, uint8_t *dst, uint32_t *nreadp) = 0;

    /** Appends data to a file asynchronously.  Issues an append request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.
     *
     * @param fd open file descriptor
     * @param buf pointer to data to append
     * @param amount amount of data to append
     * @param handler dispatch handler
     * @return Error::OK on success or error code on failure
     */
    virtual int append(int32_t fd, const void *buf, uint32_t amount, DispatchHandler *handler) = 0;
    
    /**
     * Appends data to a file.  Issues an append request and waits for it to
     * complete.
     *
     * @param fd open file descriptor
     * @param buf pointer to data to append
     * @param amount amount of data to append
     * @return Error::OK on success or error code on failure
     */
    virtual int append(int32_t fd, const void *buf, uint32_t amount) = 0;

    virtual int seek(int32_t fd, uint64_t offset, DispatchHandler *handler) = 0;
    virtual int seek(int32_t fd, uint64_t offset) = 0;

    virtual int remove(std::string &name, DispatchHandler *handler) = 0;
    virtual int remove(std::string &name) = 0;

    virtual int length(std::string &name, DispatchHandler *handler) = 0;
    virtual int length(std::string &name, int64_t *lenp) = 0;

    virtual int pread(int32_t fd, uint64_t offset, uint32_t amount, DispatchHandler *handler) = 0;
    virtual int pread(int32_t fd, uint64_t offset, uint32_t amount, uint8_t *dst, uint32_t *nreadp) = 0;

    virtual int mkdirs(std::string &name, DispatchHandler *handler) = 0;
    virtual int mkdirs(std::string &name) = 0;

    virtual int rmdir(std::string &name, DispatchHandler *handler) = 0;
    virtual int rmdir(std::string &name) = 0;

    virtual int readdir(std::string &name, DispatchHandler *handler) = 0;
    virtual int readdir(std::string &name, std::vector<std::string> &listing) = 0;

    virtual int flush(int32_t fd, DispatchHandler *handler) = 0;
    virtual int flush(int32_t fd) = 0;

  };

}


#endif // HYPERTABLE_FILESYSTEM_H

