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

#include <boost/algorithm/string/predicate.hpp>

#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"
#include "Common/md5.h"

#include "Hypertable/Lib/CompressorFactory.h"

#include "BlockCompressionCodec.h"
#include "CommitLog.h"
#include "CommitLogBlockStream.h"
#include "CommitLogReader.h"

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace std;

namespace {
  struct ByFragmentNumber {
    bool operator()(const String &x, const String &y) const {
      int num_x = atoi(x.c_str());
      int num_y = atoi(y.c_str());
      return num_x < num_y;
    }
  };
}


CommitLogReader::CommitLogReader(FilesystemPtr &fs, const String &log_dir, bool mark_for_deletion)
  : CommitLogBase(log_dir), m_fs(fs), m_fragment_queue_offset(0),
    m_block_buffer(256), m_revision(TIMESTAMP_MIN), m_compressor(0) {

  if (get_bool("Hypertable.CommitLog.SkipErrors"))
    CommitLogBlockStream::ms_assert_on_error = false;

  load_fragments(log_dir, mark_for_deletion);
  reset();
}


CommitLogReader::~CommitLogReader() {
}


bool
CommitLogReader::next_raw_block(CommitLogBlockInfo *infop,
                                BlockCompressionHeaderCommitLog *header) {
  LogFragmentQueue::iterator fragment_queue_iter;

  try_again:
  fragment_queue_iter = m_fragment_queue.begin() + m_fragment_queue_offset;
  if (fragment_queue_iter == m_fragment_queue.end())
    return false;

  if ((*fragment_queue_iter).block_stream == 0)
    (*fragment_queue_iter).block_stream =
      new CommitLogBlockStream(m_fs, (*fragment_queue_iter).log_dir,
                               format("%u", (*fragment_queue_iter).num));

  if (!(*fragment_queue_iter).block_stream->next(infop, header)) {
    CommitLogFileInfo &info = *fragment_queue_iter;
    delete info.block_stream;
    info.block_stream = 0;
    if (m_revision == TIMESTAMP_MIN) {
      HT_WARNF("Skipping log fragment '%s/%u' because unable to read any valid blocks",
               info.log_dir.c_str(), info.num);
      m_fragment_queue.erase(fragment_queue_iter);
    }
    else {
      info.revision = m_revision;
      m_fragment_queue_offset++;
    }
    m_revision = TIMESTAMP_MIN;
    goto try_again;
  }

  if (header->check_magic(CommitLog::MAGIC_LINK)) {
    assert(header->get_compression_type() == BlockCompressionCodec::NONE);
    String log_dir = (const char *)(infop->block_ptr + header->length());
    load_fragments(log_dir, true);
    m_linked_logs.insert(md5_hash(log_dir.c_str()));
    if (header->get_revision() > m_latest_revision)
      m_latest_revision = header->get_revision();
    if (header->get_revision() > m_revision)
      m_revision = header->get_revision();
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
      DynamicBuffer zblock(0, false);

      m_block_buffer.clear();
      zblock.base = binfo.block_ptr;
      zblock.ptr = binfo.block_ptr + binfo.block_len;

      try {
        load_compressor(header->get_compression_type());
        m_compressor->inflate(zblock, m_block_buffer, *header);
      }
      catch (Exception &e) {
        LogFragmentQueue::iterator iter = m_fragment_queue.begin() + m_fragment_queue_offset;
        HT_ERRORF("Inflate error in CommitLog fragment %s starting at "
                  "postion %lld (block len = %lld) - %s",
                  (*iter).block_stream->get_fname().c_str(),
                  (Lld)binfo.start_offset, (Lld)(binfo.end_offset
                  - binfo.start_offset), Error::get_text(e.code()));
        continue;
      }

      if (header->get_revision() > m_latest_revision)
        m_latest_revision = header->get_revision();

      if (header->get_revision() > m_revision)
        m_revision = header->get_revision();

      *blockp = m_block_buffer.base;
      *lenp = m_block_buffer.fill();
      return true;
    }

    LogFragmentQueue::iterator iter = m_fragment_queue.begin() + m_fragment_queue_offset;
    HT_WARNF("Corruption detected in CommitLog fragment %s starting at "
             "postion %lld for %lld bytes - %s",
             (*iter).block_stream->get_fname().c_str(),
             (Lld)binfo.start_offset, (Lld)(binfo.end_offset
             - binfo.start_offset), Error::get_text(binfo.error));
  }

  sort(m_fragment_queue.begin(), m_fragment_queue.end());

  return false;
}


void CommitLogReader::load_fragments(String log_dir, bool mark_for_deletion) {
  vector<string> listing;
  CommitLogFileInfo file_info;
  bool added_fragments = false;

  FileUtils::add_trailing_slash(log_dir);

  try {
    m_fs->readdir(log_dir, listing);
  }
  catch (Hypertable::Exception &e) {
    if (e.code() == Error::DFSBROKER_BAD_FILENAME) {
      HT_INFOF("Skipping directory '%s' because it does not exist",
               log_dir.c_str());
      return;
    }
    HT_THROW2(e.code(), e, e.what());
  }

  if (listing.size() == 0)
    return;

  sort(listing.begin(), listing.end(), ByFragmentNumber());

  for (size_t i=0; i<listing.size(); i++) {

    if (boost::ends_with(listing[i], ".tmp"))
      continue;

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
      if (file_info.size > 0) {
        m_fragment_queue.push_back(file_info);
        added_fragments = true;
      }
    }
  }

  // set the "purge log dir" bit on the most recent fragment
  if (added_fragments) {
    if (mark_for_deletion) {
      HT_ASSERT(!boost::ends_with(m_fragment_queue.back().log_dir, "user/"));
      m_fragment_queue.back().purge_log_dir = true;
    }
  }
  else if (mark_for_deletion) {
    HT_INFOF("Removing commit log directory %s because it is empty",
             log_dir.c_str());
    m_fs->rmdir(log_dir);
  }

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
