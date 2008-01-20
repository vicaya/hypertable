/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

namespace Hypertable {

  /**
   * Abstract base class for a filesystem.  All commands have synchronous and
   * asynchronous versions.  Commands that operate on the same file descriptor
   * are serialized by the underlying filesystem.  In other words, if you issue
   * three asynchronous commands, they will get carried out and their responses
   * will come back in the same order in which they were issued.
   */
  class Filesystem {
  public:

    virtual ~Filesystem() { return; }

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
     * @param buf_size read buffer size
     * @param outstanding maximum number of outstanding reads
     * @param fdp address of variable to hold return file descriptor
     * @param start_offset starting read offset
     * @param end_offset ending read offset
     * @return Error::OK on success or error code on failure
     */
    virtual int open_buffered(std::string &name, uint32_t buf_size, uint32_t outstanding, int32_t *fdp, uint64_t start_offset=0, uint64_t end_offset=0) = 0;

    /** Decodes the response from an open request
     *
     * @param event_ptr reference to response event
     * @param fdp address of variable to hold return file descriptor
     * @return error code from event object
     */
    static int decode_response_open(EventPtr &event_ptr, int32_t *fdp);

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

    /** Decodes the response from a create request
     *
     * @param event_ptr reference to response event
     * @param fdp address of variable to hold return file descriptor
     * @return error code from event object
     */
    static int decode_response_create(EventPtr &event_ptr, int32_t *fdp);

    /** Closes a file asynchronously.  Issues a close file request.
     * The caller will get notified of successful completion or error via
     * the given dispatch handler.  This command will get serialized along
     * with other commands issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @param handler dispatch handler
     * @return Error::OK on success or error code on failure
     */
    virtual int close(int32_t fd, DispatchHandler *handler) = 0;

    /** Closes a file.  Issues a close command and waits for it
     * to complete.
     * This command will get serialized along with other commands
     * issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @return Error::OK on success or error code on failure
     */
    virtual int close(int32_t fd) = 0;

    /** Reads data from a file at the current position asynchronously.  Issues
     * a read request.  The caller will get notified of successful completion or
     * error via the given dispatch handler.  It's up to the caller to deserialize
     * the returned data in the MESSAGE event object.  EOF is indicated by a short read.
     * This command will get serialized along with other commands issued with the same
     * file descriptor.
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
     * This command will get serialized along with other commands
     * issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @param amount amount of data to read
     * @param dst destination buffer for read data
     * @param nreadp address of variable to hold amount of data read.
     * @return Error::OK on success or error code on failure
     */
    virtual int read(int32_t fd, uint32_t amount, uint8_t *dst, uint32_t *nreadp) = 0;

    /** Decodes the response from a read request
     *
     * @param event_ptr reference to response event
     * @param dst destination buffer for read data
     * @param nreadp address of variable to hold amount of data read.
     * @return error code from event object
     */
    static int decode_response_read(EventPtr &event_ptr, uint8_t *dst, uint32_t *nreadp);

    /** Decodes the header from the response from a read request
     *
     * @param event_ptr reference to response event
     * @param offsetp address of variable offset of data read.
     * @param amountp address of variable to hold amount of data read.
     * @param dstp address of pointer to hold pointer to data read
     * @return error code from event object
     */
    static int decode_response_read_header(EventPtr &event_ptr, uint64_t *offsetp, uint32_t *amountp, uint8_t **dstp);

    /** Appends data to a file asynchronously.  Issues an append request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.  This command will get serialized along with
     * other commands issued with the same file descriptor.
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
     * This command will get serialized along with other commands
     * issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @param buf pointer to data to append
     * @param amount amount of data to append
     * @return Error::OK on success or error code on failure
     */
    virtual int append(int32_t fd, const void *buf, uint32_t amount) = 0;

    /** Decodes the response from an append request
     *
     * @param event_ptr reference to response event
     * @param offsetp address of variable to hold offset
     * @param amountp address of variable to hold amount of data appended
     * @return error code from event object
     */
    static int decode_response_append(EventPtr &event_ptr, uint64_t *offsetp, uint32_t *amountp);

    /** Seeks current file position asynchronously.  Issues a seek request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.  This command will get serialized along with
     * other commands issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @param offset absolute offset to seek to
     * @param handler dispatch handler
     * @return Error::OK on success or error code on failure
     */
    virtual int seek(int32_t fd, uint64_t offset, DispatchHandler *handler) = 0;

    /** Seeks current file position.  Issues a seek request and waits for it to
     * complete.
     * This command will get serialized along with other commands
     * issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @param offset absolute offset to seek to
     * @return Error::OK on success or error code on failure
     */
    virtual int seek(int32_t fd, uint64_t offset) = 0;

    /** Removes a file asynchronously.  Issues a remove request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.
     *
     * @param name absolute pathname of file to delete
     * @param handler dispatch handler
     * @return Error::OK on success or error code on failure
     */
    virtual int remove(std::string &name, DispatchHandler *handler) = 0;

    /** Removes a file.  Issues a remove request and waits for it to
     * complete.
     *
     * @param name absolute pathname of file to delete
     * @return Error::OK on success or error code on failure
     */
    virtual int remove(std::string &name) = 0;

    /** Gets the length of a file asynchronously.  Issues a length request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.
     *
     * @param name absolute pathname of file
     * @param handler dispatch handler
     * @return Error::OK on success or error code on failure
     */
    virtual int length(std::string &name, DispatchHandler *handler) = 0;

    /** Gets the length of a file.  Issues a length request and waits for it
     * to complete.
     *
     * @param name absolute pathname of file
     * @param lenp address of variable to hold returned length
     * @return Error::OK on success or error code on failure
     */
    virtual int length(std::string &name, int64_t *lenp) = 0;

    /** Decodes the response from a length request
     *
     * @param event_ptr reference to response event
     * @param lenp address of variable to hold returned length
     * @return error code from event object
     */
    static int decode_response_length(EventPtr &event_ptr, int64_t *lenp);

    /** Reads data from a file at the specified position asynchronously.  Issues
     * a pread request.  The caller will get notified of successful completion or
     * error via the given dispatch handler.  It's up to the caller to deserialize
     * the returned data in the MESSAGE event object.  EOF is indicated by a short read.
     *
     * @param fd open file descriptor
     * @param offset starting offset of read
     * @param amount amount of data to read
     * @param handler dispatch handler
     * @return Error::OK on success or error code on failure
     */
    virtual int pread(int32_t fd, uint64_t offset, uint32_t amount, DispatchHandler *handler) = 0;

    /** Reads data from a file at the specified position.  Issues a pread
     * request and waits for it to complete, returning the read data.
     * EOF is indicated by a short read.
     * This command will get serialized along with other commands
     * issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @param offset starting offset of read
     * @param amount amount of data to read
     * @param dst destination buffer for read data
     * @param nreadp address of variable to hold amount of data read.
     * @return Error::OK on success or error code on failure
     */
    virtual int pread(int32_t fd, uint64_t offset, uint32_t amount, uint8_t *dst, uint32_t *nreadp) = 0;

    /** Decodes the response from a pread request
     *
     * @param event_ptr reference to response event
     * @param dst destination buffer for read data
     * @param nreadp address of variable to hold amount of data read.
     * @return error code from event object
     */
    static int decode_response_pread(EventPtr &event_ptr, uint8_t *dst, uint32_t *nreadp);

    /** Creates a directory asynchronously.  Issues a mkdirs request which creates a directory,
     * including all its missing parents.  The caller will get notified of successful completion
     * or error via the given dispatch handler.
     *
     * @param name absolute pathname of directory to create
     * @param handler dispatch handler
     * @return Error::OK on success or error code on failure
     */
    virtual int mkdirs(std::string &name, DispatchHandler *handler) = 0;

    /** Creates a directory.  Issues a mkdirs request which creates a directory,
     * including all its missing parents, and waits for it to complete.
     *
     * @param name absolute pathname of directory to create
     * @return Error::OK on success or error code on failure
     */
    virtual int mkdirs(std::string &name) = 0;

    /** Recursively removes a directory asynchronously.  Issues a rmdir request.
     * The caller will get notified of successful completion or error via the given
     * dispatch handler.
     *
     * @param name absolute pathname of directory to remove
     * @param handler dispatch handler
     * @return Error::OK on success or error code on failure
     */
    virtual int rmdir(std::string &name, DispatchHandler *handler) = 0;

    /** Recursively removes a directory.  Issues a rmdir request and waits for
     * it to complete.
     *
     * @param name absolute pathname of directory to remove
     * @return Error::OK on success or error code on failure
     */
    virtual int rmdir(std::string &name) = 0;
    
    /** Obtains a listing of all files in a directory asynchronously.  Issues a readdir
     * request.  The caller will get notified of successful completion or error via the
     * given dispatch handler.
     *
     * @param name absolute pathname of directory
     * @param handler dispatch handler
     * @return Error::OK on success or error code on failure
     */
    virtual int readdir(std::string &name, DispatchHandler *handler) = 0;

    /** Obtains a listing of all files in a directory.  Issues a readdir request
     * and waits for it to complete.
     *
     * @param name absolute pathname of directory
     * @param listing reference to vector of entry names
     * @return Error::OK on success or error code on failure
     */
    virtual int readdir(std::string &name, std::vector<std::string> &listing) = 0;

    /** Decodes the response from a readdir request
     *
     * @param event_ptr reference to response event
     * @param listing reference to vector of entry names
     * @return error code from event object
     */
    static int decode_response_readdir(EventPtr &event_ptr, std::vector<std::string> &listing);

    /** Flushes a file asynchronously.  Isues a flush command which causes all buffered
     * writes to get persisted to disk.  The caller will get notified of successful
     * completion or error via the given dispatch handler.  This command will get
     * serialized along with other commands issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @param handler dispatch handler
     * @return Error::OK on success or error code on failure
     */
    virtual int flush(int32_t fd, DispatchHandler *handler) = 0;

    /** Flushes a file.  Isues a flush command which causes all buffered
     * writes to get persisted to disk, and waits for it to complete.
     * This command will get serialized along with other commands issued
     * with the same file descriptor.
     *
     * @param fd open file descriptor
     * @return Error::OK on success or error code on failure
     */
    virtual int flush(int32_t fd) = 0;

    /** Decodes the response from an request that only returns an error code
     *
     * @param event_ptr reference to response event
     * @return error code from event object
     */
    static int decode_response(EventPtr &event_ptr);

  };

}


#endif // HYPERTABLE_FILESYSTEM_H

