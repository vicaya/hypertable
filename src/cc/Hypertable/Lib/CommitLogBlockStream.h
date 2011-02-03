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

#ifndef HYPERTABLE_COMMITLOGBLOCKSTREAM_H
#define HYPERTABLE_COMMITLOGBLOCKSTREAM_H

#include "Common/DynamicBuffer.h"
#include "Common/ReferenceCount.h"
#include "Common/String.h"
#include "Common/Filesystem.h"

#include "BlockCompressionHeaderCommitLog.h"


namespace Hypertable {

  class CommitLogBlockInfo {
  public:
    const char *log_dir;
    const char *file_fragment;
    uint8_t *block_ptr;
    size_t block_len;
    uint64_t start_offset;
    uint64_t end_offset;
    int error;
  };


  class CommitLogBlockStream : public ReferenceCount {

  public:

    CommitLogBlockStream(FilesystemPtr &fs);
    CommitLogBlockStream(FilesystemPtr &fs, const String &log_dir,
                         const String &fragment);
    virtual ~CommitLogBlockStream();

    void load(const String &log_dir, const String &fragment);
    void close();
    bool next(CommitLogBlockInfo *, BlockCompressionHeaderCommitLog *);

    String &get_fname() { return m_fname; }

    static bool ms_assert_on_error;

  private:

    int load_next_valid_header(BlockCompressionHeaderCommitLog *header);

    FilesystemPtr m_fs;
    String        m_fragment;
    String        m_fname;
    String        m_log_dir;
    int32_t       m_fd;
    uint64_t      m_cur_offset;
    uint64_t      m_file_length;
    DynamicBuffer m_block_buffer;
  };

  typedef intrusive_ptr<CommitLogBlockStream> CommitLogBlockStreamPtr;

}

#endif // HYPERTABLE_COMMITLOGBLOCKSTREAM_H

