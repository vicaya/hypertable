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

#ifndef HYPERTABLE_COMMITLOG_H
#define HYPERTABLE_COMMITLOG_H

#include <deque>
#include <map>
#include <string>

#include <boost/thread/xtime.hpp>

extern "C" {
#include <sys/time.h>
}

#include <boost/thread/mutex.hpp>

#include "Common/Properties.h"
#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/BlockCompressionCodec.h"
#include "Hypertable/Lib/Filesystem.h"

namespace Hypertable {

  typedef struct {
    std::string  fname;
    uint64_t     size;
    uint64_t     timestamp;
  } CommitLogFileInfoT;

  typedef struct {
    uint32_t distance;
    uint64_t cumulative_size;
  } FragmentPriorityDataT;

  /**
   * Commit log for persisting updates.  The commit log is a directory that contains
   * a growing number of files that contain compressed blocks of "commits".  The files
   * are named starting with '0' and will periodically roll, which means that a trailer
   * is written into the file, the file is closed, and then the numeric name is
   * incremented by one and opened.  Periodically when old parts of the log are no
   * longer needed, they get purged.  The size of each file is determined by the
   * following config file property:
   *<pre>
   * Hypertable.RangeServer.logFileSize
   *</pre>
   * The commit log will persist (write then sync) a series of "commits" which are either
   * blocks of updates (serialized sequence of key/value pairs) or a link block which is
   * a block that just contains an external reference to another commit log that should be
   * linked into this commit log at the point where the link occurs.  Each commit is
   * compressed into a block using the same block compressor object that are used for
   * the cell stores.  By default, the quicklz algorithm is used, but this can be changed
   * with The following config property:
   *<pre>
   *Hypertable.RangeServer.CommitLog.Compressor
   *</pre>
   * Each block begins with a header and contains the following information:
   *<pre>
   * magic_string (10 bytes)
   * header length (2 bytes)
   * uncompressed length (4 bytes)
   * compressed length (4 bytes)
   * compression type (2 bytes)
   * checksum (2 bytes)
   * timestamp (8 bytes)
   * '\0' terminated table_name (variable)
   *</pre>
   * When the log rolls, a header (with an empty table name) is written at the end of the
   * file and contains the timestamp of the most recent commit in the file.
   */
  class CommitLog : public ReferenceCount {
  public:

    CommitLog(Filesystem *fs, std::string &log_dir, PropertiesPtr &props_ptr);
    virtual ~CommitLog();

    /**
     * Atomically obtains a timestamp
     * 
     * @return microseconds since the epoch
     */
    uint64_t get_timestamp();

    /** Writes a block of updates to the commit log.
     *
     * @param table_name name of table that the external log applies to
     * @param data pointer to block of updates
     * @param len length of block of updates
     * @param timestamp current commit log time obtained with a call to #get_timestamp
     * @return Error::OK on success or error code on failure
     */
    int write(const char *table_name, uint8_t *data, uint32_t len, uint64_t timestamp);

    /** Links an external log into this log.
     *
     * @param table_name name of table that the external log applies to
     * @param log_dir the directory of the external log
     * @param timestamp current commit log time obtained with a call to #get_timestamp
     * @return Error::OK on success or error code on failure
     */
    int link_log(const char *table_name, const char *log_dir, uint64_t timestamp);

    int close(uint64_t timestamp);
    int purge(uint64_t timestamp);
    std::string &get_log_dir() { return m_log_dir; }

    void load_fragment_priority_map(std::map<uint64_t, FragmentPriorityDataT> &frag_map);

    int64_t get_max_fragment_size() { return m_max_file_size; }

    static const char MAGIC_UPDATES[10];
    static const char MAGIC_LINK[10];
    static const char MAGIC_TRAILER[10];

  private:

    int roll();
    int compress_and_write(DynamicBuffer &input, BlockCompressionHeader *header, uint64_t timestamp);

    boost::mutex               m_mutex;
    Filesystem                *m_fs;
    BlockCompressionCodec     *m_compressor;
    std::string                m_log_dir;
    std::string                m_log_file;
    int64_t                    m_max_file_size;
    int64_t                    m_cur_log_length;
    uint32_t                   m_cur_log_num;
    int32_t                    m_fd;
    uint64_t                   m_last_timestamp;
    std::deque<CommitLogFileInfoT>   m_file_info_queue;
  };

  typedef boost::intrusive_ptr<CommitLog> CommitLogPtr;
  
}

#endif // HYPERTABLE_COMMITLOG_H

