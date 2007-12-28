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
#ifndef HYPERTABLE_COMMITLOGREADER_H
#define HYPERTABLE_COMMITLOGREADER_H

#include <string>
#include <vector>

#include <boost/thread/mutex.hpp>

#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/Filesystem.h"
#include "Hypertable/Lib/Key.h"

#include "CommitLog.h"
#include "CommitLogReader.h"

using namespace Hypertable;

namespace Hypertable {

  typedef struct {
    uint32_t     num;
    std::string  fname;
    int64_t      length;
    uint64_t     timestamp;
  } LogFileInfoT;

  inline bool operator<(const LogFileInfoT &lfi1, const LogFileInfoT &lfi2) {
    return lfi1.num < lfi2.num;
  }

  class CommitLogReader : public ReferenceCount {
  public:
    CommitLogReader(Filesystem *fs, std::string &logDir);
    virtual ~CommitLogReader() { return; }
    virtual void initialize_read(uint64_t timestamp);
    virtual bool next_block(CommitLogHeaderT **blockp);
    virtual int last_error() { return m_error; }

  private:
    Filesystem               *m_fs;
    std::string               m_log_dir;
    std::vector<LogFileInfoT> m_log_file_info;
    uint64_t                  m_cutoff_time;
    size_t                    m_cur_log_offset;
    int32_t                   m_fd;
    DynamicBuffer             m_block_buffer;
    int                       m_error;
  };
  typedef boost::intrusive_ptr<CommitLogReader> CommitLogReaderPtr;

}

#endif // HYPERTABLE_COMMITLOGREADER_H

