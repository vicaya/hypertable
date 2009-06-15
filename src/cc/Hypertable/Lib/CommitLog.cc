/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

#include "Common/Checksum.h"
#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"
#include "Common/Config.h"

#include "AsyncComm/Protocol.h"

#include "Hypertable/Lib/CompressorFactory.h"
#include "Hypertable/Lib/BlockCompressionCodec.h"
#include "Hypertable/Lib/BlockCompressionHeaderCommitLog.h"

#include "Defaults.h"
#include "CommitLog.h"
#include "CommitLogReader.h"

using namespace Hypertable;

const char CommitLog::MAGIC_DATA[10] =
    { 'C','O','M','M','I','T','D','A','T','A' };
const char CommitLog::MAGIC_LINK[10] =
    { 'C','O','M','M','I','T','L','I','N','K' };

namespace {
  struct forward_sort_clfi {
    bool
    operator()(const CommitLogFileInfo &x, const CommitLogFileInfo &y) const {
      return x.num < y.num;
    }
  };
}


CommitLog::CommitLog(Filesystem *fs, const String &log_dir)
  : CommitLogBase(log_dir) {
  initialize(fs, log_dir, Config::properties, 0);
}

CommitLog::~CommitLog() {
  delete m_compressor;
  close();
}

void
CommitLog::initialize(Filesystem *fs, const String &log_dir,
                      PropertiesPtr &props, CommitLogBase *init_log) {
  String compressor;
  bool flush;

  m_fs = fs;
  m_log_dir = log_dir;
  m_cur_fragment_length = 0;
  m_cur_fragment_num = 0;
  m_needs_roll = false;

  SubProperties cfg(props, "Hypertable.CommitLog.");

  HT_TRY("getting commit log properites",
    m_max_fragment_size = cfg.get_i64("RollLimit");
    compressor = cfg.get_str("Compressor");
    flush = cfg.get_bool("Flush"));

  m_flush_flag = (flush) ? Filesystem::O_FLUSH : 0;

  m_compressor = CompressorFactory::create_block_codec(compressor);

  FileUtils::add_trailing_slash(m_log_dir);

  if (init_log) {
    stitch_in(init_log);
    foreach (const CommitLogFileInfo &frag, m_fragment_queue) {
      if (frag.num >= m_cur_fragment_num)
        m_cur_fragment_num = frag.num + 1;
    }
  }
  else {  // chose one past the max one found in the directory
    uint32_t num;
    std::vector<String> listing;
    m_fs->readdir(m_log_dir, listing);
    for (size_t i=0; i<listing.size(); i++) {
      num = atoi(listing[i].c_str());
      if (num >= m_cur_fragment_num)
        m_cur_fragment_num = num + 1;
    }
  }

  m_cur_fragment_fname = m_log_dir + m_cur_fragment_num;

  try {
    m_fs->mkdirs(m_log_dir);
    m_fd = m_fs->create(m_cur_fragment_fname, true, 8192, 3, 67108864);
  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("Problem initializing commit log '%s' - %s (%s)",
              m_log_dir.c_str(), e.what(), Error::get_text(e.code()));
    m_fd = -1;
    throw;
  }
}


int64_t CommitLog::get_timestamp() {
  ScopedLock lock(m_mutex);
  boost::xtime now;
  boost::xtime_get(&now, boost::TIME_UTC);
  return ((int64_t)now.sec * 1000000000LL) + (int64_t)now.nsec;
}


int CommitLog::write(DynamicBuffer &buffer, int64_t revision) {
  int error;
  BlockCompressionHeaderCommitLog header(MAGIC_DATA, revision);

  if (m_needs_roll) {
    ScopedLock lock(m_mutex);
    if ((error = roll()) != Error::OK)
      return error;
  }

  /**
   * Compress and write the commit block
   */
  if ((error = compress_and_write(buffer, &header, revision)) != Error::OK)
    return error;

  /**
   * Roll the log
   */
  if (m_cur_fragment_length > m_max_fragment_size) {
    ScopedLock lock(m_mutex);
    roll();
  }

  return Error::OK;
}


int CommitLog::link_log(CommitLogBase *log_base) {
  int error;
  BlockCompressionHeaderCommitLog header(MAGIC_LINK,
                                         log_base->get_latest_revision());
  DynamicBuffer input;
  String &log_dir = log_base->get_log_dir();

  if (m_needs_roll) {
    ScopedLock lock(m_mutex);
    if ((error = roll()) != Error::OK)
      return error;
  }

  input.ensure(header.length());

  header.set_compression_type(BlockCompressionCodec::NONE);
  header.set_data_length(log_dir.length() + 1);
  header.set_data_zlength(log_dir.length() + 1);
  header.set_data_checksum(fletcher32(log_dir.c_str(), log_dir.length()+1));

  header.encode(&input.ptr);
  input.add(log_dir.c_str(), log_dir.length() + 1);

  try {
    ScopedLock lock(m_mutex);
    size_t amount = input.fill();
    StaticBuffer send_buf(input);

    m_fs->append(m_fd, send_buf, m_flush_flag);
    m_cur_fragment_length += amount;

    roll();

  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("Problem linking external log into commit log - %s", e.what());
    return e.code();
  }

  return Error::OK;
}


int CommitLog::close() {

  try {
    ScopedLock lock(m_mutex);
    if (m_fd > 0) {
      m_fs->close(m_fd);
      m_fd = -1;
    }
  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("Problem closing commit log file '%s' - %s (%s)",
              m_cur_fragment_fname.c_str(), e.what(),
              Error::get_text(e.code()));
    return e.code();
  }

  return Error::OK;
}


int CommitLog::purge(int64_t revision) {
  ScopedLock lock(m_mutex);
  CommitLogFileInfo file_info;
  String fname;

  try {

    while (!m_fragment_queue.empty()) {
      file_info = m_fragment_queue.front();
      if (file_info.revision < revision) {
        fname = file_info.log_dir + file_info.num;
        m_fs->remove(fname);
        m_fragment_queue.pop_front();
        HT_INFOF("clgc Removed log fragment file='%s' revision=%lld", fname.c_str(),
                 (Lld)file_info.revision);
        if (file_info.purge_log_dir) {
          HT_INFOF("Removing commit log directory %s", file_info.log_dir.c_str());
          m_fs->rmdir(file_info.log_dir);
        }
      }
      else {

        HT_INFOF("clgc LOG FRAGMENT PURGE breaking because %lld >= %lld",
                 (Lld)file_info.revision, (Lld)revision);

        break;
      }
    }

  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("Problem purging log fragment fname = '%s'", fname.c_str());
    return e.code();
  }

  return Error::OK;
}


int CommitLog::roll() {
  CommitLogFileInfo file_info;

  if (m_latest_revision == 0)
    return Error::OK;

  m_needs_roll = true;

  if (m_fd > 0) {
    try {
      m_fs->close(m_fd);
    }
    catch (Exception &e) {
      if (e.code() != Error::DFSBROKER_BAD_FILE_HANDLE) {
        HT_ERRORF("Problem closing commit log fragment: %s: %s",
                  m_cur_fragment_fname.c_str(), e.what());
        return e.code();
      }
    }

    m_fd = -1;

    file_info.log_dir = m_log_dir;
    file_info.num = m_cur_fragment_num;
    file_info.size = m_cur_fragment_length;
    file_info.revision = m_latest_revision;
    file_info.purge_log_dir = false;
    file_info.block_stream = 0;

    if (m_fragment_queue.empty() || m_fragment_queue.back().revision
        < file_info.revision)
      m_fragment_queue.push_back(file_info);
    else {
      m_fragment_queue.push_back(file_info);
      sort(m_fragment_queue.begin(), m_fragment_queue.end());
    }

    m_latest_revision = 0;
    m_cur_fragment_length = 0;

    m_cur_fragment_num++;
    m_cur_fragment_fname = m_log_dir + m_cur_fragment_num;

  }

  try {
    m_fd = m_fs->create(m_cur_fragment_fname, true, 8192, 3, 67108864);
  }
  catch (Exception &e) {
    HT_ERRORF("Problem rolling commit log: %s: %s",
              m_cur_fragment_fname.c_str(), e.what());
    return e.code();
  }

  m_needs_roll = false;

  return Error::OK;
}


int
CommitLog::compress_and_write(DynamicBuffer &input,
    BlockCompressionHeader *header, int64_t revision) {
  int error = Error::OK;
  EventPtr event_ptr;
  DynamicBuffer zblock;

  // Compress block and kick off log write (protected by lock)
  try {
    ScopedLock lock(m_mutex);

    m_compressor->deflate(input, zblock, *header);

    size_t amount = zblock.fill();
    StaticBuffer send_buf(zblock);

    m_fs->append(m_fd, send_buf, m_flush_flag);
    assert(revision != 0);
    if (revision > m_latest_revision)
      m_latest_revision = revision;
    m_cur_fragment_length += amount;
  }
  catch (Exception &e) {
    HT_ERRORF("Problem writing commit log: %s: %s",
              m_cur_fragment_fname.c_str(), e.what());
    error = e.code();
  }

  return error;
}


void CommitLog::load_cumulative_size_map(CumulativeSizeMap &cumulative_size_map) {
  ScopedLock lock(m_mutex);
  int64_t cumulative_total = 0;
  uint32_t distance = 0;
  CumulativeFragmentData frag_data;

  memset(&frag_data, 0, sizeof(frag_data));

  if (m_latest_revision != 0) {
    frag_data.size = m_cur_fragment_length;
    frag_data.fragno = m_cur_fragment_num;
    cumulative_size_map[m_latest_revision] = frag_data;
  }

  for (std::deque<CommitLogFileInfo>::reverse_iterator iter
       = m_fragment_queue.rbegin(); iter != m_fragment_queue.rend(); ++iter) {
    frag_data.size = (*iter).size;
    frag_data.fragno = (*iter).num;
    cumulative_size_map[(*iter).revision] = frag_data;
  }

  for (CumulativeSizeMap::reverse_iterator riter = cumulative_size_map.rbegin();
       riter != cumulative_size_map.rend(); riter++) {
    (*riter).second.distance = distance++;
    cumulative_total += (*riter).second.size;
    (*riter).second.cumulative_size = cumulative_total;
  }

}


void CommitLog::get_stats(String &stats) {
  ScopedLock lock(m_mutex);

  try {
    foreach (const CommitLogFileInfo &frag, m_fragment_queue) {
      stats += String("STAT frag-") + frag.num + "\tsize\t" + frag.size + "\n";
      stats += String("STAT frag-") + frag.num + "\trevision\t" + frag.revision + "\n";
    }
    stats += String("STAT frag-") + m_cur_fragment_num + "\tsize\t" + m_cur_fragment_length + "\n";
    stats += String("STAT frag-") + m_cur_fragment_num + "\trevision\t" + m_latest_revision + "\n";
  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << "Problem getting stats for log fragments" << HT_END;
    HT_THROW(e.code(), e.what());
  }
}

