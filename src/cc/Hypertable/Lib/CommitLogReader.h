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

#ifndef HYPERTABLE_COMMITLOGREADER_H
#define HYPERTABLE_COMMITLOGREADER_H

#include <vector>

#include <boost/thread/mutex.hpp>

#include "Common/ReferenceCount.h"
#include "Common/String.h"

#include "BlockCompressionHeaderCommitLog.h"
#include "CommitLog.h"
#include "CommitLogReader.h"
#include "Filesystem.h"
#include "Key.h"

using namespace Hypertable;

namespace Hypertable {

  class CommitLogReader : public ReferenceCount {

  public:
    CommitLogReader(Filesystem *fs, String logDir);
    virtual ~CommitLogReader();

    bool next_block(const uint8_t **blockp, size_t *lenp, BlockCompressionHeaderCommitLog *header);

    void dump_log_metadata();

  private:

    void load_fragments(String &logDir);
    void load_compressor(uint16_t ztype);

    Filesystem       *m_fs;
    String            m_log_dir;
    LogFragmentQueue  m_fragment_queue;
    LogFragmentStack  m_fragment_stack;
    size_t            m_cur_log_offset;
    DynamicBuffer     m_block_buffer;

    typedef __gnu_cxx::hash_map<uint16_t, BlockCompressionCodecPtr> CompressorMap;

    CompressorMap          m_compressor_map;
    uint16_t               m_compressor_type;
    BlockCompressionCodec *m_compressor;

  };
  typedef boost::intrusive_ptr<CommitLogReader> CommitLogReaderPtr;

}

#endif // HYPERTABLE_COMMITLOGREADER_H

