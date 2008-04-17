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

#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/Protocol.h"

#include "Hypertable/Lib/CompressorFactory.h"
#include "Hypertable/Lib/BlockCompressionHeaderCommitLog.h"

#include "CommitLog.h"

using namespace Hypertable;

const char CommitLog::MAGIC_UPDATES[10] = { 'L','O','G','U','P','D','A','T','E','S' };
const char CommitLog::MAGIC_LINK[10]    = { 'L','O','G','L','I','N','K','-','-','-' };
const char CommitLog::MAGIC_TRAILER[10] = { 'L','O','G','T','R','A','I','L','E','R' };


CommitLog::~CommitLog() {
  delete m_compressor;
  if (m_fd > 0) {
    try {
      m_fs->close(m_fd);
    }
    catch (Exception &e) {
      HT_ERRORF("Problem closing commit log file '%s' - %s",
                m_log_file.c_str(), e.what());
    }
  }
}

void CommitLog::initialize(Filesystem *fs, const std::string &log_dir, PropertiesPtr &props_ptr) {
  std::string compressor;

  m_fs = fs;
  m_log_dir = log_dir;
  m_cur_log_length = 0;
  m_cur_log_num = 0;
  m_last_timestamp = 0;

  if (props_ptr) {
    m_max_file_size = props_ptr->get_int64("Hypertable.RangeServer.CommitLog.RollLimit", 100000000LL);
    compressor = props_ptr->get("Hypertable.RangeServer.CommitLog.Compressor", "lzo");
  }
  else {
    m_max_file_size = 268435456LL;
    compressor = "lzo";
  }

  HT_INFOF("RollLimit = %lld", m_max_file_size);

  m_compressor = CompressorFactory::create_block_codec(compressor);

  //cout << "Hypertable.RangeServer.logFileSize=" << logFileSize << endl;

  if (m_log_dir.find('/', m_log_dir.length()-1) == string::npos)
    m_log_dir += "/";

  m_log_file = m_log_dir + m_cur_log_num;

  // create the log directory
  m_fs->mkdirs(m_log_dir);

  m_fd = m_fs->create(m_log_file, true, 8192, 3, 67108864);
}




/**
 *
 */
uint64_t CommitLog::get_timestamp() {
  boost::mutex::scoped_lock lock(m_mutex);
  boost::xtime now;
  boost::xtime_get(&now, boost::TIME_UTC);
  return ((uint64_t)now.sec * 1000000000LL) + (uint64_t)now.nsec;
}




/**
 * 
 */
int CommitLog::write(uint8_t *data, uint32_t len, uint64_t timestamp) {
  int error;
  BlockCompressionHeaderCommitLog header(MAGIC_UPDATES, timestamp);
  DynamicBuffer input(0);

  input.buf = data;
  input.ptr = data + len;

  /**
   * Compress and write the commit block
   */
  if ((error = compress_and_write(input, &header, timestamp)) != Error::OK) {
    input.release();
    return error;
  }

  input.release();

  /**
   * Roll the log
   */
  if (m_cur_log_length > m_max_file_size)
    error = roll();
  else
    error = Error::OK;

  return error;
}



/** This method links an external log into this log by first rolling the log.
 * The current log file is closed and a new one is opened.  The linked to log
 * directory is written as the first log entry in the newly opened file.
 */
int CommitLog::link_log(const char *log_dir, uint64_t timestamp) {
  int error;
  BlockCompressionHeaderCommitLog header(MAGIC_LINK, timestamp);
  DynamicBuffer input(0);

  /**
   * Roll the log
   */
  if ((error = roll()) != Error::OK)
    return error;

  input.add(log_dir, strlen(log_dir)+1);

  /**
   * Compress and write the commit block
   */
  if ((error = compress_and_write(input, &header, timestamp)) != Error::OK) {
    input.release();
    return error;
  }

  input.release();

  return Error::OK;
}



/**
 * 
 */
int CommitLog::close(uint64_t timestamp) {
  BlockCompressionHeaderCommitLog header(MAGIC_TRAILER, timestamp);
  DynamicBuffer trailer(0);

  header.set_compression_type( m_compressor->get_type() );

  trailer.ensure(header.length());
  header.encode(&trailer.ptr);

  try {
    boost::mutex::scoped_lock lock(m_mutex);

    m_fs->append(m_fd, trailer.buf, header.length());
    trailer.release();
    m_fs->flush(m_fd);
    m_fs->close(m_fd);
    m_fd = 0;
  }
  catch (Exception &e) {
      HT_ERRORF("Problem closing commit log file '%s' - %s", m_log_file.c_str(),
                e.what());
  }

  return Error::OK;
}



/**
 * 
 */
int CommitLog::purge(uint64_t timestamp) {
  boost::mutex::scoped_lock lock(m_mutex);
  CommitLogFileInfo fileInfo;

  while (!m_file_info_queue.empty()) {
    fileInfo = m_file_info_queue.front();
    if (fileInfo.timestamp > 0 && fileInfo.timestamp < timestamp) {
      m_fs->remove(fileInfo.fname);
      m_file_info_queue.pop_front();
      HT_INFOF("Removed log fragment file='%s' timestamp=%lld", fileInfo.fname.c_str(), fileInfo.timestamp);
    }
    else {
      //HT_INFOF("LOG FRAGMENT PURGE breaking because %lld >= %lld", fileInfo.timestamp, timestamp);
      break;
    }
  }
  return Error::OK;
}


/**
 *
 */
int CommitLog::roll() {
  CommitLogFileInfo fileInfo;
  DynamicBuffer trailer(0);

  try {
    boost::mutex::scoped_lock lock(m_mutex);
    BlockCompressionHeaderCommitLog header(MAGIC_TRAILER, m_last_timestamp);

    header.set_compression_type( m_compressor->get_type() );

    trailer.ensure(header.length());
    header.encode(&trailer.ptr);

    m_fs->append(m_fd, trailer.buf, header.length());
    trailer.release();
    m_fs->flush(m_fd);
    m_fs->close(m_fd);

    fileInfo.timestamp = m_last_timestamp;
    fileInfo.size = m_cur_log_length + header.length();
    fileInfo.fname = m_log_file;
    m_file_info_queue.push_back(fileInfo);

    m_last_timestamp = 0;
    m_cur_log_length = 0;

    m_cur_log_num++;
    m_log_file = m_log_dir + m_cur_log_num;

    m_fd = m_fs->create(m_log_file, true, 8192, 3, 67108864);
  }
  catch (Exception &e) {
    HT_ERRORF("Problem rolling commit log: %s: %s",
              m_log_file.c_str(), e.what());
    return e.code();
  }

  return Error::OK;
}



/**
 *
 */
int CommitLog::compress_and_write(DynamicBuffer &input, BlockCompressionHeader *header, uint64_t timestamp) {
  int error = Error::OK;
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  DynamicBuffer zblock(0);

  // Compress block and kick off log write (protected by lock)
  try {
    boost::mutex::scoped_lock lock(m_mutex);
    size_t zlen;

    if ((error = m_compressor->deflate(input, zblock, *header)) != Error::OK)
      return error;

    m_fs->append(m_fd, zblock.buf, zblock.fill(), &sync_handler);
    zblock.release(&zlen);
    m_fs->flush(m_fd, &sync_handler);
    m_last_timestamp = timestamp;
    m_cur_log_length += zlen;
  }
  catch (Exception &e) {
    HT_ERRORF("Problem writing commit log: %s: %s",
              m_log_file.c_str(), e.what());
    error = e.code();
  }

  // wait for append to complete
  if (!sync_handler.wait_for_reply(event_ptr)) {
    HT_ERRORF("Problem appending to commit log file '%s' - %s",
              m_log_file.c_str(),
              Protocol::string_format_message(event_ptr).c_str());
    error = (int)Protocol::response_code(event_ptr);
  }

  // wait for flush to complete
  if (!sync_handler.wait_for_reply(event_ptr)) {
    HT_ERRORF("Problem flushing commit log file '%s' - %s",
              m_log_file.c_str(),
              Protocol::string_format_message(event_ptr).c_str());
    error = (int)Protocol::response_code(event_ptr);
  }

  return error;
}



/**
 *
 */
void CommitLog::load_fragment_priority_map(LogFragmentPriorityMap &frag_map) {
  boost::mutex::scoped_lock lock(m_mutex);
  uint64_t cumulative_total = m_cur_log_length;
  uint32_t distance = 0;
  LogFragmentPriorityData frag_data;

  frag_data.distance = distance++;
  frag_data.cumulative_size = cumulative_total;
  frag_map[m_last_timestamp] = frag_data;

  for (deque<CommitLogFileInfo>::reverse_iterator iter = m_file_info_queue.rbegin(); iter != m_file_info_queue.rend(); iter++) {
    cumulative_total += (*iter).size;
    frag_data.distance = distance++;
    frag_data.cumulative_size = cumulative_total;
    frag_map[(*iter).timestamp] = frag_data;
  }
  
}
