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

#include "Common/Compat.h"
#include <cassert>
#include <vector>

extern "C" {
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
}

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"

#include "Hypertable/Lib/CompressorFactory.h"

#include "BlockCompressionCodec.h"
#include "CommitLog.h"
#include "CommitLogBlockStream.h"
#include "CommitLogReader.h"

using namespace Hypertable;
using namespace std;

namespace {
  struct ByReverseClFi {
    bool
    operator()(const CommitLogFileInfo &x, const CommitLogFileInfo &y) const {
      return x.num >= y.num;
    }
  };
}


CommitLogReader::CommitLogReader(Filesystem *fs, const String &log_dir)
  : CommitLogBase(log_dir), m_fs(fs), m_block_buffer(256), m_revision(0),
    m_compressor(0) {
  load_fragments(log_dir);
}


CommitLogReader::~CommitLogReader() {
}


bool
CommitLogReader::next_raw_block(CommitLogBlockInfo *infop,
                                BlockCompressionHeaderCommitLog *header) {
 try_again:

  if (m_fragment_stack.empty())
    return false;

  if (m_fragment_stack.top().block_stream == 0)
    m_fragment_stack.top().block_stream =
        new CommitLogBlockStream(m_fs, m_fragment_stack.top().log_dir,
                                 format("%u", m_fragment_stack.top().num));

  if (!m_fragment_stack.top().block_stream->next(infop, header)) {
    delete m_fragment_stack.top().block_stream;
    m_fragment_stack.top().block_stream = 0;
    m_fragment_stack.top().revision = m_revision;
    m_fragment_queue.push_back(m_fragment_stack.top());
    m_fragment_stack.pop();
    m_revision = 0;
    goto try_again;
  }

  if (header->check_magic(CommitLog::MAGIC_LINK)) {
    assert(header->get_compression_type() == BlockCompressionCodec::NONE);
    String log_dir = (const char *)(infop->block_ptr + header->length());
    load_fragments(log_dir);
    goto try_again;
  }

  return true;
}


bool
CommitLogReader::next(const uint8_t **blockp, size_t *lenp,
                      BlockCompressionHeaderCommitLog *header) {
  CommitLogBlockInfo binfo;

  while (next_raw_block(&binfo, header)) {

    if (binfo.error == Error::OK) {
      DynamicBuffer zblock;

      m_block_buffer.clear();
      zblock.base = binfo.block_ptr;
      zblock.ptr = binfo.block_ptr + binfo.block_len;

      try {
        load_compressor(header->get_compression_type());
        m_compressor->inflate(zblock, m_block_buffer, *header);
      }
      catch (Exception &e) {
        HT_ERRORF("Inflate error in CommitLog fragment %s starting at "
                  "postion %lld (block len = %lld) - %s",
                  m_fragment_stack.top().block_stream->get_fname().c_str(),
                  binfo.start_offset, binfo.end_offset - binfo.start_offset,
                  Error::get_text(e.code()));
        continue;
      }

      if (header->get_revision() > m_latest_revision)
        m_latest_revision = header->get_revision();

      if (header->get_revision() > m_revision)
        m_revision = header->get_revision();

      zblock.release();
      *blockp = m_block_buffer.base;
      *lenp = m_block_buffer.fill();
      return true;
    }

    HT_WARNF("Corruption detected in CommitLog fragment %s starting at "
             "postion %lld for %lld bytes - %s",
             m_fragment_stack.top().block_stream->get_fname().c_str(),
             binfo.start_offset, binfo.end_offset - binfo.start_offset,
             Error::get_text(binfo.error));
    m_fragment_stack.pop();
  }

  sort(m_fragment_queue.begin(), m_fragment_queue.end());

  return false;
}


void CommitLogReader::load_fragments(String log_dir) {
  vector<string> listing;
  CommitLogFileInfo file_info;
  vector<CommitLogFileInfo> fragment_vector;

  FileUtils::add_trailing_slash(log_dir);

  m_fs->readdir(log_dir, listing);

  if (listing.size() == 0)
    return;

  for (size_t i=0; i<listing.size(); i++) {
    char *endptr;
    long num = strtol(listing[i].c_str(), &endptr, 10);
    if (*endptr != 0) {
      HT_WARNF("Invalid file '%s' found in commit log directory '%s'",
               listing[i].c_str(), log_dir.c_str());
    }
    else {
      file_info.num = (uint32_t)num;
      file_info.log_dir = log_dir;
      file_info.purge_log_dir = false;
      file_info.revision = 0;
      file_info.block_stream = 0;
      file_info.size = m_fs->length(log_dir + listing[i]);
      if (file_info.size > 0)
        fragment_vector.push_back(file_info);
    }
  }

  if (fragment_vector.empty())
    return;

  sort(fragment_vector.begin(), fragment_vector.end(), ByReverseClFi());

  // set the "purge log dir" bit on the most recent fragment
  fragment_vector[0].purge_log_dir = true;

  for (size_t i=0; i<fragment_vector.size(); i++)
    m_fragment_stack.push(fragment_vector[i]);
}


void CommitLogReader::load_compressor(uint16_t ztype) {
  BlockCompressionCodecPtr compressor_ptr;

  if (m_compressor != 0 && ztype == m_compressor_type)
    return;

  if (ztype >= BlockCompressionCodec::COMPRESSION_TYPE_LIMIT)
    HT_THROWF(Error::BLOCK_COMPRESSOR_UNSUPPORTED_TYPE,
              "Invalid compression type '%d'", (int)ztype);

  compressor_ptr = m_compressor_map[ztype];

  if (!compressor_ptr) {
    compressor_ptr = CompressorFactory::create_block_codec(
        (BlockCompressionCodec::Type)ztype);
    m_compressor_map[ztype] = compressor_ptr;
  }

  m_compressor_type = ztype;
  m_compressor = compressor_ptr.get();
}
