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

#include <queue>
#include <string>

#include <boost/thread/xtime.hpp>

extern "C" {
#include <sys/time.h>
}

#include <boost/thread/mutex.hpp>

#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/Filesystem.h"

namespace Hypertable {

  typedef struct {
    char      marker[8];
    uint64_t  timestamp;
    uint32_t  length;
    uint16_t  checksum;
    uint16_t  flags;
  } __attribute__((packed)) CommitLogHeaderT;

  typedef struct {
    std::string  fname;
    uint64_t     timestamp;
  } CommitLogFileInfoT;

  /**
   * Commit log for persisting updates.  Consists of a series of blocks.  Each block contains a
   * series of one or more key/value pairs that were commtted together.  Each block begins with
   * a header with the following format:
   * <pre>
   * "-BLOCK--"   - 8 bytes
   * timestamp    - 8 bytes
   * length       - 4 bytes
   * checksum     - 2 bytes
   * tableName    - variable
   * '\\0'         - 1 byte
   * payload      - variable
   * </pre>
   *
   * The log ends with an empty block containing the timestamp of the last real block.
   */
  class CommitLog : public ReferenceCount {
  public:

    enum Flag { LINK=0x0001 };

    CommitLog(Filesystem *fs, std::string &logDir, int64_t logFileSize);
    virtual ~CommitLog() { return; }

    int write(const char *tableName, uint8_t *data, uint32_t len, uint64_t timestamp);

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

    uint64_t get_timestamp() { 
      boost::mutex::scoped_lock lock(m_mutex);
      boost::xtime now;
      boost::xtime_get(&now, boost::TIME_UTC);
      return ((uint64_t)now.sec * 1000000000LL) + (uint64_t)now.nsec;
    }

  private:

    boost::mutex               m_mutex;
    Filesystem                *m_fs;
    std::string                m_log_dir;
    std::string                m_log_file;
    int64_t                    m_max_file_size;
    int64_t                    m_cur_log_length;
    uint32_t                   m_cur_log_num;
    int32_t                    m_fd;
    uint64_t                   m_last_timestamp;
    std::queue<CommitLogFileInfoT>   m_file_info_queue;
  };

  typedef boost::intrusive_ptr<CommitLog> CommitLogPtr;
  
}

#endif // HYPERTABLE_COMMITLOG_H

