/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_FILESYSTEM_H
#define HYPERTABLE_FILESYSTEM_H

#include <vector>
#include "Common/String.h"
#include "Common/StaticBuffer.h"
#include "Common/ReferenceCount.h"
#include "AsyncComm/DispatchHandler.h"

#define HT_DIRECT_IO_ALIGNMENT 512
#define HT_IO_ALIGNED(size) (((size) % HT_DIRECT_IO_ALIGNMENT) == 0)
#define HT_IO_ALIGNMENT_PADDING(size) (HT_DIRECT_IO_ALIGNMENT-((size)%HT_DIRECT_IO_ALIGNMENT))

namespace Hypertable {

  /**
   * Abstract base class for a filesystem.  All commands have synchronous and
   * asynchronous versions.  Commands that operate on the same file descriptor
   * are serialized by the underlying filesystem. In other words, if you issue
   * three asynchronous commands, they will get carried out and their responses
   * will come back in the same order in which they were issued. Unless other-
   * wise mentioned, the methods could throw Exception.
   */
  class Filesystem : public ReferenceCount {
  public:
    enum OptionType { O_FLUSH = 1 };
    enum OpenFlags { OPEN_FLAG_DIRECTIO = 0x00000001, OPEN_FLAG_OVERWRITE = 0x00000002 };

    virtual ~Filesystem() { return; }

    /** Opens a file asynchronously.  Issues an open file request.  The caller
     * will get notified of successful completion or error via the given
     * dispatch handler. It is up to the caller to deserialize the returned
     * file descriptor from the MESSAGE event object.
     *
     * @param name absolute path name of file to open
     * @param flags open flags (DIRECT)
     * @param handler dispatch handler
     */
    virtual void open(const String &name, uint32_t flags, DispatchHandler *handler) = 0;

    /** Opens a file.  Issues an open file request and waits for it to complete.
     *
     * @param name absolute path name of file to open
     * @param flags open flags (DIRECT)
     * @return file descriptor
     */
    virtual int open(const String &name, uint32_t flags=0) = 0;

    /** Opens a file in buffered (readahead) mode.  Issues an open file request
     * and waits for it to complete. Turns on readahead mode so that data is
     * prefetched.
     *
     * @param name absolute path name of file to open
     * @param flags open flags (DIRECT)
     * @param buf_size read buffer size
     * @param outstanding maximum number of outstanding reads
     * @param start_offset starting read offset
     * @param end_offset ending read offset
     * @return file descriptor
     */
    virtual int open_buffered(const String &name, uint32_t flags, 
                              uint32_t buf_size, uint32_t outstanding,
                              uint64_t start_offset=0, uint64_t end_offset=0) = 0;

    /** Decodes the response from an open request
     *
     * @param event_ptr reference to response event
     * @return file descriptor
     */
    static int decode_response_open(EventPtr &event_ptr);

    /** Creates a file asynchronously.  Issues a create file request with
     * various create mode parameters. The caller will get notified of
     * successful completion or error via the given dispatch handler.  It is
     * up to the caller to deserialize the returned file descriptor from the
     * MESSAGE event object.
     *
     * @param name absolute path name of file to open
     * @param flags open flags (DIRECT, OVERWRITE)
     * @param bufsz buffer size to use for the underlying FS
     * @param replication replication factor to use for this file
     * @param blksz block size to use for the underlying FS
     * @param handler dispatch handler
     */
    virtual void create(const String &name, uint32_t flags, int32_t bufsz,
                        int32_t replication, int64_t blksz,
                        DispatchHandler *handler) = 0;

    /** Creates a file.  Issues a create file request and waits for completion
     *
     * @param name absolute path name of file to open
     * @param flags open flags (DIRECT, OVERWRITE)
     * @param bufsz buffer size to use for the underlying FS
     * @param replication replication factor to use for this file
     * @param blksz block size to use for the underlying FS
     * @return file descriptor
     */
    virtual int create(const String &name, uint32_t flags, int32_t bufsz,
                       int32_t replication, int64_t blksz) = 0;

    /** Decodes the response from a create request
     *
     * @param event_ptr reference to response event
     * @return file descriptor
     */
    static int decode_response_create(EventPtr &event_ptr);

    /** Closes a file asynchronously.  Issues a close file request.
     * The caller will get notified of successful completion or error via
     * the given dispatch handler.  This command will get serialized along
     * with other commands issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @param handler dispatch handler
     */
    virtual void close(int fd, DispatchHandler *handler) = 0;

    /** Closes a file.  Issues a close command and waits for it
     * to complete.
     * This command will get serialized along with other commands
     * issued with the same file descriptor.
     *
     * @param fd open file descriptor
     */
    virtual void close(int fd) = 0;

    /** Reads data from a file at the current position asynchronously.  Issues
     * a read request.  The caller will get notified of successful completion or
     * error via the given dispatch handler.  It's up to the caller to
     * deserialize the returned data in the MESSAGE event object.  EOF is
     * indicated by a short read. This command will get serialized along with
     * other commands issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @param len amount of data to read
     * @param handler dispatch handler
     */
    virtual void read(int fd, size_t len, DispatchHandler *handler) = 0;

    /** Reads data from a file at the current position.  Issues a read
     * request and waits for it to complete, returning the read data.
     * EOF is indicated by a short read.
     * This command will get serialized along with other commands
     * issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @param dst destination buffer for read data
     * @param len amount of data to read
     * @return amount read
     */
    virtual size_t read(int fd, void *dst, size_t len) = 0;

    /** Decodes the response from a read request
     *
     * @param event_ptr reference to response event
     * @param dst destination buffer for read data
     * @param len destination buffer size
     * @return amount read
     */
    static size_t decode_response_read(EventPtr &event_ptr,
                                       void *dst, size_t len);

    /** Decodes the header from the response from a read request
     *
     * @param event_ptr reference to response event
     * @param offsetp address of variable offset of data read.
     * @param dstp address of pointer to hold pointer to data read
     * @return amount read
     */
    static size_t decode_response_read_header(EventPtr &event_ptr,
                                              uint64_t *offsetp,
                                              uint8_t **dstp = 0);

    /** Appends data to a file asynchronously.  Issues an append request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.  This command will get serialized along with
     * other commands issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @param buffer buffer to append
     * @param flags - O_FLUSH or 0
     * @param handler dispatch handler
     */
    virtual void append(int fd, StaticBuffer &buffer, uint32_t flags,
                        DispatchHandler *handler) = 0;

    /**
     * Appends data to a file.  Issues an append request and waits for it to
     * complete.
     * This command will get serialized along with other commands
     * issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @param buffer buffer to append
     * @param flags - O_FLUSH or 0
     */
    virtual size_t append(int fd, StaticBuffer &buffer, uint32_t flags = 0) = 0;

    /** Decodes the response from an append request
     *
     * @param event_ptr reference to response event
     * @param offsetp address of variable to hold offset
     * @return amount appended
     */
    static size_t decode_response_append(EventPtr &event_ptr,
                                         uint64_t *offsetp);

    /** Seeks current file position asynchronously.  Issues a seek request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.  This command will get serialized along with
     * other commands issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @param offset absolute offset to seek to
     * @param handler dispatch handler
     */
    virtual void seek(int fd, uint64_t offset, DispatchHandler *handler) = 0;

    /** Seeks current file position.  Issues a seek request and waits for it to
     * complete.
     * This command will get serialized along with other commands
     * issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @param offset absolute offset to seek to
     */
    virtual void seek(int fd, uint64_t offset) = 0;

    /** Removes a file asynchronously.  Issues a remove request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.
     *
     * @param name absolute pathname of file to delete
     * @param handler dispatch handler
     */
    virtual void remove(const String &name, DispatchHandler *handler) = 0;

    /** Removes a file.  Issues a remove request and waits for it to
     * complete.
     *
     * @param name absolute pathname of file to delete
     * @param force ignore non-existence error
     */
    virtual void remove(const String &name, bool force = true) = 0;

    /** Gets the length of a file asynchronously.  Issues a length request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.
     *
     * @param name absolute pathname of file
     * @param handler dispatch handler
     * @return Error::OK on success or error code on failure
     */
    virtual void length(const String &name, DispatchHandler *handler) = 0;

    /** Gets the length of a file.  Issues a length request and waits for it
     * to complete.
     *
     * @param name absolute pathname of file
     */
    virtual int64_t length(const String &name) = 0;

    /** Decodes the response from a length request
     *
     * @param event_ptr reference to response event
     * @return length
     */
    static int64_t decode_response_length(EventPtr &event_ptr);

    /** Reads data from a file at the specified position asynchronously.
     * Issues a pread request.  The caller will get notified of successful
     * completion or error via the given dispatch handler.  It's up to the
     * caller to deserialize the returned data in the MESSAGE event object.
     * EOF is indicated by a short read.
     *
     * @param fd open file descriptor
     * @param offset starting offset of read
     * @param amount amount of data to read
     * @param handler dispatch handler
     */
    virtual void pread(int fd, size_t amount, uint64_t offset,
                       DispatchHandler *handler) = 0;

    /** Reads data from a file at the specified position.  Issues a pread
     * request and waits for it to complete, returning the read data.
     * EOF is indicated by a short read.
     * This command will get serialized along with other commands
     * issued with the same file descriptor.
     *
     * @param fd open file descriptor
     * @param offset starting offset of read
     * @param len amount of data to read
     * @param dst destination buffer for read data
     * @return amount of data read
     */
    virtual size_t pread(int fd, void *dst, size_t len, uint64_t offset) = 0;

    /** Decodes the response from a pread request
     *
     * @param event_ptr reference to response event
     * @param dst destination buffer for read data
     * @param len destination buffer size
     * @return amount of data read
     */
    static size_t decode_response_pread(EventPtr &event_ptr,
                                        void *dst, size_t len);

    /** Creates a directory asynchronously.  Issues a mkdirs request which
     * creates a directory, including all its missing parents.  The caller
     * will get notified of successful completion or error via the given
     * dispatch handler.
     *
     * @param name absolute pathname of directory to create
     * @param handler dispatch handler
     */
    virtual void mkdirs(const String &name, DispatchHandler *handler) = 0;

    /** Creates a directory.  Issues a mkdirs request which creates a directory,
     * including all its missing parents, and waits for it to complete.
     *
     * @param name absolute pathname of directory to create
     */
    virtual void mkdirs(const String &name) = 0;

    /** Recursively removes a directory asynchronously.  Issues a rmdir request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.
     *
     * @param name absolute pathname of directory to remove
     * @param handler dispatch handler
     */
    virtual void rmdir(const String &name, DispatchHandler *handler) = 0;

    /** Recursively removes a directory.  Issues a rmdir request and waits for
     * it to complete.
     *
     * @param name absolute pathname of directory to remove
     * @param force don't throw an error if file does not exist
     */
    virtual void rmdir(const String &name, bool force = true) = 0;

    /** Obtains a listing of all files in a directory asynchronously.  Issues a
     * readdir request.  The caller will get notified of successful completion
     * or error via the given dispatch handler.
     *
     * @param name absolute pathname of directory
     * @param handler dispatch handler
     */
    virtual void readdir(const String &name, DispatchHandler *handler) = 0;

    /** Obtains a listing of all files in a directory.  Issues a readdir request
     * and waits for it to complete.
     *
     * @param name absolute pathname of directory
     * @param listing reference to vector of entry names
     */
    virtual void readdir(const String &name, std::vector<String> &listing) = 0;

    /** Decodes the response from a readdir request
     *
     * @param event_ptr reference to response event
     * @param listing reference to vector of entry names
     */
    static void decode_response_readdir(EventPtr &event_ptr,
                                        std::vector<String> &listing);

    /** Flushes a file asynchronously.  Isues a flush command which causes all
     * buffered writes to get persisted to disk.  The caller will get notified
     * of successful completion or error via the given dispatch handler.  This
     * command will get serialized along with other commands issued with the
     * same file descriptor.
     *
     * @param fd open file descriptor
     * @param handler dispatch handler
     */
    virtual void flush(int fd, DispatchHandler *handler) = 0;

    /** Flushes a file.  Isues a flush command which causes all buffered
     * writes to get persisted to disk, and waits for it to complete.
     * This command will get serialized along with other commands issued
     * with the same file descriptor.
     *
     * @param fd open file descriptor
     */
    virtual void flush(int fd) = 0;

    /** Determines if a file exists asynchronously.  Issues an exists request.
     * The caller will get notified of successful completion or error via the
     * given dispatch handler.
     *
     * @param name absolute pathname of file
     * @param handler dispatch handler
     */
    virtual void exists(const String &name, DispatchHandler *handler) = 0;

    /** Determines if a file exists.
     *
     * @param name absolute pathname of file
     * @return true/false
     */
    virtual bool exists(const String &name) = 0;

    /** Decodes the response from an exists request.
     *
     * @param event_ptr reference to response event
     * @return true if file exists, false otherwise
     */
    static bool decode_response_exists(EventPtr &event_ptr);

    /** Rename path asynchronously
     *
     * @param src - source path
     * @param dst - destination path
     * @param handler - dispatch/callback handler
     */
    virtual void rename(const String &src, const String &dst,
                        DispatchHandler *handler) = 0;

    /** Rename a path
     *
     * @param src - source path
     * @param dst - destination path
     */
    virtual void rename(const String &src, const String &dst) = 0;

    /** Decodes the response from an request that only returns an error code
     *
     * @param event_ptr reference to response event
     * @return error code
     */
    static int decode_response(EventPtr &event_ptr);

    /** Invokes debug request asynchronously
     *
     * @param command debug command identifier
     * @param serialized_parameters command specific serialized parameters
     */
    virtual void debug(int32_t command,
                       StaticBuffer &serialized_parameters) = 0;

    /** Invokes debug request
     *
     * @param command debug command identifier
     * @param serialized_parameters command specific serialized parameters
     * @param handler - dispatch/callback handler
     */
    virtual void debug(int32_t command, StaticBuffer &serialized_parameters,
                       DispatchHandler *handler) = 0;
  };

  typedef intrusive_ptr<Filesystem> FilesystemPtr;

} // namespace Hypertable


#endif // HYPERTABLE_FILESYSTEM_H

