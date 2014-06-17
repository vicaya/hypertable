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

#ifndef HYPERTABLE_COMMITLOGBASE_H
#define HYPERTABLE_COMMITLOGBASE_H

#include <deque>

#include "Common/String.h"

#include "KeySpec.h"
#include "CommitLogBlockStream.h"

namespace Hypertable {

  typedef struct {
    String     log_dir;
    uint32_t   num;
    uint64_t   size;
    int64_t    revision;
    bool       purge_log_dir;
    CommitLogBlockStream *block_stream;
  } CommitLogFileInfo;

  /**
   * Less than operator for LocationCacheKey
   */
  inline bool
  operator<(const CommitLogFileInfo &x, const CommitLogFileInfo &y) {
    return x.revision < y.revision;
  }

  typedef std::deque<CommitLogFileInfo> LogFragmentQueue;

  /**
   */
  class CommitLogBase : public ReferenceCount {
  public:
    CommitLogBase(const String &log_dir)
      : m_log_dir(log_dir), m_latest_revision(TIMESTAMP_MIN) {
      size_t lastslash = log_dir.find_last_of('/');

      if (lastslash == log_dir.length()-1)
        lastslash = log_dir.find_last_of('/', log_dir.length()-2);

      m_log_name = (lastslash == String::npos) ? log_dir
                                               : log_dir.substr(lastslash+1);
    }

    void stitch_in(CommitLogBase *other) {
      for (LogFragmentQueue::iterator iter = other->m_fragment_queue.begin();
           iter != other->m_fragment_queue.end(); iter++)
        m_fragment_queue.push_back(*iter);
    }

    String &get_log_dir() { return m_log_dir; }

    int64_t get_latest_revision() { return m_latest_revision; }

    bool empty() { return m_fragment_queue.empty(); }

    std::set<int64_t> &get_linked_log_set() { return m_linked_logs; }

  protected:
    String            m_log_dir;
    String            m_log_name;
    LogFragmentQueue  m_fragment_queue;
    int64_t           m_latest_revision;
    std::set<int64_t> m_linked_logs;
  };

  typedef intrusive_ptr<CommitLogBase> CommitLogBasePtr;

} // namespace Hypertable

#endif // HYPERTABLE_COMMITLOGBASE_H

