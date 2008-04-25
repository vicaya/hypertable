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

#include "Common/Checksum.h"
#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/Protocol.h"

#include "Hypertable/Lib/CompressorFactory.h"
#include "Hypertable/Lib/BlockCompressionCodec.h"
#include "Hypertable/Lib/BlockCompressionHeaderCommitLog.h"

#include "CommitLog.h"
#include "CommitLogReader.h"

using namespace Hypertable;

const char CommitLog::MAGIC_DATA[10] = { 'C','O','M','M','I','T','D','A','T','A' };
const char CommitLog::MAGIC_LINK[10] = { 'C','O','M','M','I','T','L','I','N','K' };


CommitLog::~CommitLog() {
  delete m_compressor;
  close();
}

void CommitLog::initialize(Filesystem *fs, const String &log_dir, PropertiesPtr &props_ptr) {
  String compressor;
  m_fs = fs;
  m_log_dir = log_dir;
  m_cur_fragment_length = 0;
  m_cur_fragment_num = 0;
  m_last_timestamp = 0;

  if (props_ptr) {
    m_max_fragment_size = props_ptr->get_int64("Hypertable.RangeServer.CommitLog.RollLimit", 100000000LL);
    compressor = props_ptr->get("Hypertable.RangeServer.CommitLog.Compressor", "lzo");
  }
  else {
    m_max_fragment_size = 268435456LL;
    compressor = "lzo";
  }

  HT_INFOF("RollLimit = %lld", m_max_fragment_size);

  m_compressor = CompressorFactory::create_block_codec(compressor);

  FileUtils::add_trailing_slash( m_log_dir );

  m_cur_fragment_fname = m_log_dir + m_cur_fragment_num;

  try {
    m_fs->mkdirs(m_log_dir);
    m_fd = m_fs->create(m_cur_fragment_fname, true, 8192, 3, 67108864);
  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("Problem initializing commit log '%s' - %s (%s)", m_log_dir.c_str(), e.what(), Error::get_text(e.code()));
    throw;
  }
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
  BlockCompressionHeaderCommitLog header(MAGIC_DATA, timestamp);
  DynamicBuffer input(0, false);

  input.buf = data;
  input.ptr = data + len;

  /**
   * Compress and write the commit block
   */
  if ((error = compress_and_write(input, &header, timestamp)) != Error::OK)
    return error;

  /**
   * Roll the log
   */
  if (m_cur_fragment_length > m_max_fragment_size) {
    boost::mutex::scoped_lock lock(m_mutex);
    error = roll();
  }
  else
    error = Error::OK;

  return error;
}



/**
 */
int CommitLog::link_log(CommitLogBase *log_base, uint64_t timestamp) {
  int error;
  BlockCompressionHeaderCommitLog header(MAGIC_LINK, timestamp);
  DispatchHandlerSynchronizer sync_handler;
  DynamicBuffer input(0, false);
  String &log_dir = log_base->get_log_dir();

  input.ensure( header.length() );

  header.set_compression_type(BlockCompressionCodec::NONE);
  header.set_data_length( log_dir.length() + 1 );
  header.set_data_zlength( log_dir.length() + 1 );
  header.set_data_checksum( fletcher32(log_dir.c_str(), log_dir.length()+1) );

  header.encode(&input.ptr);
  input.add( log_dir.c_str(), log_dir.length() + 1 );

  try {
    boost::mutex::scoped_lock lock(m_mutex);

    m_fs->append(m_fd, input.buf, input.fill(), &sync_handler);
    m_fs->flush(m_fd, &sync_handler);
    m_last_timestamp = timestamp;
    m_cur_fragment_length += input.fill();

    if ((error = roll()) != Error::OK)
      return error;

    // Stitch in the fragment queue from the log being linked
    // in to the current fragment queue of the current log
    stitch_in( log_base );

  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("Problem linking external log into commit log - %s", e.what());
    return e.code();
  }

  return Error::OK;
}



/**
 */
int CommitLog::close() {

  try {
    boost::mutex::scoped_lock lock(m_mutex);
    if (m_fd > 0)
      m_fs->close(m_fd);
  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("Problem closing commit log file '%s' - %s (%s)",
	      m_cur_fragment_fname.c_str(), e.what(), Error::get_text(e.code()));
    return e.code();
  }
    
  m_fd = 0;

  return Error::OK;
}



/**
 * 
 */
int CommitLog::purge(uint64_t timestamp) {
  boost::mutex::scoped_lock lock(m_mutex);
  CommitLogFileInfo file_info;
  String fname;

  try {

    while (!m_fragment_queue.empty()) {
      file_info = m_fragment_queue.front();
      if (file_info.timestamp > 0 && file_info.timestamp < timestamp) {
	fname = file_info.log_dir + file_info.num;
	m_fs->remove(fname);
	m_fragment_queue.pop_front();
	HT_INFOF("Removed log fragment file='%s' timestamp=%lld", fname.c_str(), file_info.timestamp);
      }
      else {
	//HT_INFOF("LOG FRAGMENT PURGE breaking because %lld >= %lld", file_info.timestamp, timestamp);
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


/**
 *
 */
int CommitLog::roll() {
  CommitLogFileInfo file_info;

  try {

    m_fs->close(m_fd);

    file_info.log_dir = m_log_dir;
    file_info.num = m_cur_fragment_num;
    file_info.size = m_cur_fragment_length;
    file_info.timestamp = m_last_timestamp;
    file_info.purge_log_dir = false;
    file_info.block_stream = 0;

    m_fragment_queue.push_back(file_info);

    m_last_timestamp = 0;
    m_cur_fragment_length = 0;

    m_cur_fragment_num++;
    m_cur_fragment_fname = m_log_dir + m_cur_fragment_num;

    m_fd = m_fs->create(m_cur_fragment_fname, true, 8192, 3, 67108864);
  }
  catch (Exception &e) {
    HT_ERRORF("Problem rolling commit log: %s: %s",
              m_cur_fragment_fname.c_str(), e.what());
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
  DynamicBuffer zblock(0, false);

  // Compress block and kick off log write (protected by lock)
  try {
    boost::mutex::scoped_lock lock(m_mutex);

    if ((error = m_compressor->deflate(input, zblock, *header)) != Error::OK)
      return error;

    m_fs->append(m_fd, zblock.buf, zblock.fill(), &sync_handler);
    m_fs->flush(m_fd, &sync_handler);
    m_last_timestamp = timestamp;
    m_cur_fragment_length += zblock.fill();
  }
  catch (Exception &e) {
    HT_ERRORF("Problem writing commit log: %s: %s",
              m_cur_fragment_fname.c_str(), e.what());
    error = e.code();
  }

  // wait for append to complete
  if (!sync_handler.wait_for_reply(event_ptr)) {
    HT_ERRORF("Problem appending to commit log file '%s' - %s",
		 m_cur_fragment_fname.c_str(), Protocol::string_format_message(event_ptr).c_str());
    error = (int)Protocol::response_code(event_ptr);
  }

  // wait for flush to complete
  if (!sync_handler.wait_for_reply(event_ptr)) {
    HT_ERRORF("Problem flushing commit log file '%s' - %s",
		 m_cur_fragment_fname.c_str(), Protocol::string_format_message(event_ptr).c_str());
    error = (int)Protocol::response_code(event_ptr);
  }

  HT_INFOF("scooby\t%s\t%u\t%u", m_log_name.c_str(), header->get_data_checksum(), header->get_data_zlength());

  return error;
}



/**
 *
 */
void CommitLog::load_fragment_priority_map(LogFragmentPriorityMap &frag_map) {
  boost::mutex::scoped_lock lock(m_mutex);
  uint64_t cumulative_total = m_cur_fragment_length;
  uint32_t distance = 0;
  LogFragmentPriorityData frag_data;

  frag_data.distance = distance++;
  frag_data.cumulative_size = cumulative_total;
  frag_map[m_last_timestamp] = frag_data;

  for (deque<CommitLogFileInfo>::reverse_iterator iter = m_fragment_queue.rbegin(); iter != m_fragment_queue.rend(); iter++) {
    cumulative_total += (*iter).size;
    frag_data.distance = distance++;
    frag_data.cumulative_size = cumulative_total;
    frag_map[(*iter).timestamp] = frag_data;
  }
  
}
