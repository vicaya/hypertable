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

#include <boost/algorithm/string.hpp>

#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/Protocol.h"

#include "Hypertable/Lib/BlockCompressionCodecLzo.h"
#include "Hypertable/Lib/BlockCompressionCodecNone.h"
#include "Hypertable/Lib/BlockCompressionCodecQuicklz.h"
#include "Hypertable/Lib/BlockCompressionCodecZlib.h"
#include "Hypertable/Lib/BlockCompressionHeaderCommitLog.h"

#include "CommitLog.h"

using namespace Hypertable;

const char CommitLog::MAGIC_UPDATES[10] = { 'L','O','G','U','P','D','A','T','E','S' };
const char CommitLog::MAGIC_LINK[10]    = { 'L','O','G','L','I','N','K','-','-','-' };
const char CommitLog::MAGIC_TRAILER[10] = { 'L','O','G','T','R','A','I','L','E','R' };


/**
 *
 */
CommitLog::CommitLog(Filesystem *fs, std::string &log_dir, PropertiesPtr &props_ptr) : m_fs(fs), m_log_dir(log_dir), m_cur_log_length(0), m_cur_log_num(0), m_last_timestamp(0) {
  int error;
  std::string compressor;
  std::string compressor_type;
  std::string compressor_args;

  if (props_ptr) {
    m_max_file_size = props_ptr->getPropertyInt64("Hypertable.RangeServer.CommitLog.RollLimit", 0x100000000LL);
    compressor = props_ptr->getProperty("Hypertable.RangeServer.CommitLog.Compressor", "quicklz");
  }
  else {
    m_max_file_size = 0x100000000LL;
    compressor = "quicklz";
  }

  LOG_VA_INFO("RollLimit = %lld", m_max_file_size);

  size_t offset = compressor.find_first_of(" \t\n\r");
  if (offset != std::string::npos) {
    compressor_type = compressor.substr(0, offset);
    compressor_args = compressor.substr(offset+1);
    boost::trim(compressor_args);
  }
  else {
    compressor_type = compressor;
    compressor_args = "";
  }

  if (compressor_type == "quicklz")
    m_compressor = new BlockCompressionCodecQuicklz(compressor_args);
  else if (compressor_type == "zlib")
    m_compressor = new BlockCompressionCodecZlib(compressor_args);
  else if (compressor_type == "lzo")
    m_compressor = new BlockCompressionCodecLzo(compressor_args);
  else if (compressor_type == "none")
    m_compressor = new BlockCompressionCodecNone(compressor_args);
  else {
    LOG_VA_ERROR("Unsupported compression type - %d", compressor_type.c_str());
    DUMP_CORE;
  }

  //cout << "Hypertable.RangeServer.logFileSize=" << logFileSize << endl;

  if (m_log_dir.find('/', m_log_dir.length()-1) == string::npos)
    m_log_dir += "/";

  m_log_file = m_log_dir + m_cur_log_num;

  if ((error = m_fs->create(m_log_file, true, 8192, 3, 67108864, &m_fd)) != Error::OK) {
    LOG_VA_ERROR("Problem creating commit log file '%s' - %s", m_log_file.c_str(), Error::get_text(error));
    exit(1);
  }

}


CommitLog::~CommitLog() {
  delete m_compressor;
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
int CommitLog::write(const char *table_name, uint8_t *data, uint32_t len, uint64_t timestamp) {
  int error;
  BlockCompressionHeaderCommitLog header(MAGIC_UPDATES, timestamp, table_name);
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
int CommitLog::link_log(const char *table_name, const char *log_dir, uint64_t timestamp) {
  int error;
  BlockCompressionHeaderCommitLog header(MAGIC_LINK, timestamp, table_name);
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
  int error;
  BlockCompressionHeaderCommitLog header(MAGIC_TRAILER, timestamp, 0);
  DynamicBuffer trailer(0);

  header.set_type( m_compressor->get_type() );

  trailer.ensure(header.encoded_length());
  header.encode(&trailer.ptr);

  {
    boost::mutex::scoped_lock lock(m_mutex);

    if ((error = m_fs->append(m_fd, trailer.buf, header.fixed_length())) != Error::OK) {
      LOG_VA_ERROR("Problem appending %d byte trailer to commit log file '%s' - %s",
		   header.fixed_length(), m_log_file.c_str(), Error::get_text(error));
      return error;
    }

    trailer.release();

    if ((error = m_fs->flush(m_fd)) != Error::OK) {
      LOG_VA_ERROR("Problem flushing commit log file '%s' - %s", m_log_file.c_str(), Error::get_text(error));
      return error;
    }

    if ((error = m_fs->close(m_fd)) != Error::OK) {
      LOG_VA_ERROR("Problem closing commit log file '%s' - %s", m_log_file.c_str(), Error::get_text(error));
      return error;
    }
    
    m_fd = 0;
  }

  return Error::OK;
}



/**
 * 
 */
int CommitLog::purge(uint64_t timestamp) {
  CommitLogFileInfoT fileInfo;
  int error = Error::OK;

  while (!m_file_info_queue.empty()) {
    fileInfo = m_file_info_queue.front();
    if (fileInfo.timestamp > 0 && fileInfo.timestamp < timestamp) {
      if ((error = m_fs->remove(fileInfo.fname)) != Error::OK && error != Error::DFSBROKER_FILE_NOT_FOUND) {
	LOG_VA_ERROR("Problem removing log fragment '%s' - %s", fileInfo.fname.c_str(), Error::get_text(error));
	return error;
      }
      m_file_info_queue.pop();
      LOG_VA_INFO("Removed log fragment file='%s' timestamp=%lld", fileInfo.fname.c_str(), fileInfo.timestamp);
    }
    else {
      //LOG_VA_INFO("LOG FRAGMENT PURGE breaking because %lld >= %lld", fileInfo.timestamp, timestamp);
      break;
    }
  }
  return Error::OK;
}


/**
 *
 */
int CommitLog::roll() {
  int error;
  CommitLogFileInfoT fileInfo;
  DynamicBuffer trailer(0);

  {
    boost::mutex::scoped_lock lock(m_mutex);
    BlockCompressionHeaderCommitLog header(MAGIC_TRAILER, m_last_timestamp, 0);

    header.set_type( m_compressor->get_type() );

    trailer.ensure(header.encoded_length());
    header.encode(&trailer.ptr);

    if ((error = m_fs->append(m_fd, trailer.buf, header.fixed_length())) != Error::OK) {
      LOG_VA_ERROR("Problem appending %d bytes to commit log file '%s' - %s", trailer.fill(), m_log_file.c_str(), Error::get_text(error));
      return error;
    }

    trailer.release();

    if ((error = m_fs->flush(m_fd)) != Error::OK) {
      LOG_VA_ERROR("Problem flushing commit log file '%s' - %s", m_log_file.c_str(), Error::get_text(error));
      return error;
    }

    if ((error = m_fs->close(m_fd)) != Error::OK) {
      LOG_VA_ERROR("Problem closing commit log file '%s' - %s", m_log_file.c_str(), Error::get_text(error));
      return error;
    }

    fileInfo.timestamp = m_last_timestamp;
    fileInfo.fname = m_log_file;
    m_file_info_queue.push(fileInfo);

    m_last_timestamp = 0;
    m_cur_log_length = 0;

    m_cur_log_num++;
    m_log_file = m_log_dir + m_cur_log_num;

    if ((error = m_fs->create(m_log_file, true, 8192, 3, 67108864, &m_fd)) != Error::OK) {
      LOG_VA_ERROR("Problem creating commit log file '%s' - %s", m_log_file.c_str(), Error::get_text(error));
      return error;
    }
  }

  return Error::OK;

}



/**
 *
 */
int CommitLog::compress_and_write(DynamicBuffer &input, BlockCompressionHeader *header, uint64_t timestamp) {
  int error;
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  DynamicBuffer zblock(0);

  // Compress block and kick off log write (protected by lock)
  {
    boost::mutex::scoped_lock lock(m_mutex);
    size_t zlen;

    if ((error = m_compressor->deflate(input, zblock, header)) != Error::OK)
      return error;

    if ((error = m_fs->append(m_fd, zblock.buf, zblock.fill(), &sync_handler)) != Error::OK)
      return error;

    zblock.release(&zlen);

    if ((error = m_fs->flush(m_fd, &sync_handler)) != Error::OK)
      return error;

    m_last_timestamp = timestamp;
    m_cur_log_length += zlen;
  }

  // wait for append to complete
  if (!sync_handler.wait_for_reply(event_ptr)) {
    LOG_VA_ERROR("Problem appending to commit log file '%s' - %s",
		 m_log_file.c_str(), Protocol::string_format_message(event_ptr).c_str());
    return (int)Protocol::response_code(event_ptr);
  }

  // wait for flush to complete
  if (!sync_handler.wait_for_reply(event_ptr)) {
    LOG_VA_ERROR("Problem flushing commit log file '%s' - %s",
		 m_log_file.c_str(), Protocol::string_format_message(event_ptr).c_str());
    return (int)Protocol::response_code(event_ptr);
  }

  return Error::OK;
}


