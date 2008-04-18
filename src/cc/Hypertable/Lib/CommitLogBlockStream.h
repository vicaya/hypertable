/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#include "BlockCompressionHeaderCommitLog.h"
#include "Filesystem.h"

using namespace Hypertable;

namespace Hypertable {

  class CommitLogBlockStream : public ReferenceCount {

  public:

    typedef struct {
      uint64_t start_offset;
      uint64_t end_offset;
      int error;
    } BlockInfo;

    CommitLogBlockStream(Filesystem *fs);
    CommitLogBlockStream(Filesystem *fs, const String &fname);
    virtual ~CommitLogBlockStream();

    void load(const String &fname);
    void close();
    bool next(uint8_t **blockp, size_t *lenp, BlockInfo *infop);

    String &get_fname() { return m_fname; }

  private:

    int load_next_valid_header();
    
    Filesystem   *m_fs;
    String        m_fname;
    int32_t       m_fd;
    uint64_t      m_cur_offset;
    uint64_t      m_file_length;
    BlockCompressionHeaderCommitLog m_header;
    bool m_got_header;
    DynamicBuffer m_block_buffer;
  };
  typedef boost::intrusive_ptr<CommitLogBlockStream> CommitLogBlockStreamPtr;

}

#endif // HYPERTABLE_COMMITLOGBLOCKSTREAM_H

