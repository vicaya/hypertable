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

#include <boost/shared_array.hpp>

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/Protocol.h"

#include "CommitLog.h"

using namespace Hypertable;

/**
 *
 */
CommitLog::CommitLog(Filesystem *fs, std::string &logDir, int64_t logFileSize) : m_fs(fs), m_log_dir(logDir), m_max_file_size(logFileSize), m_cur_log_length(0), m_cur_log_num(0), m_last_timestamp(0) {
  int error;

  if (m_log_dir.find('/', m_log_dir.length()-1) == string::npos)
    m_log_dir += "/";

  m_log_file = m_log_dir + m_cur_log_num;

  if ((error = m_fs->create(m_log_file, true, 8192, 3, 67108864, &m_fd)) != Error::OK) {
    LOG_VA_ERROR("Problem creating commit log file '%s' - %s", m_log_file.c_str(), Error::get_text(error));
    exit(1);
  }

}



/**
 * 
 */
int CommitLog::write(const char *tableName, uint8_t *data, uint32_t len, uint64_t timestamp) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  uint32_t totalLen = sizeof(CommitLogHeaderT) + strlen(tableName) + 1 + len;
  uint8_t *block = new uint8_t [ totalLen ];
  CommitLogHeaderT *header = (CommitLogHeaderT *)block;
  uint8_t *ptr, *endptr;
  uint16_t checksum = 0;
  int error;

  // marker
  memcpy(header->marker, "-BLOCK--", 8);
  header->timestamp = timestamp;
  header->length = totalLen;
  header->checksum = 0;
  header->flags = 0;

  // '\0' terminated table name
  ptr = block + sizeof(CommitLogHeaderT);
  strcpy((char *)ptr, tableName);
  ptr += strlen((char *)ptr) + 1;

  memcpy(ptr, data, len);

  // compute checksum
  endptr = block + totalLen;
  for (ptr=block; ptr<endptr; ptr++)
    checksum += *ptr;
  header->checksum = checksum;

  // kick off log write (protected by lock)
  {
    boost::mutex::scoped_lock lock(m_mutex);
    if ((error = m_fs->append(m_fd, block, totalLen, &sync_handler)) != Error::OK)
      return error;
    if ((error = m_fs->flush(m_fd, &sync_handler)) != Error::OK)
      return error;
    m_last_timestamp = timestamp;
    m_cur_log_length += totalLen;
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

  /**
   * Roll the log
   */
  if (m_cur_log_length > m_max_file_size) {
    CommitLogFileInfoT fileInfo;

    /**
     *  Write trailing empty block
     */
    header = new CommitLogHeaderT[1];
    memcpy(header->marker, "-BLOCK--", 8);
    header->checksum = 0;
    header->timestamp = m_last_timestamp;
    header->length = sizeof(CommitLogHeaderT);
    header->flags = 0;

    checksum = 0;
    endptr = (uint8_t *)&header[1];
    for (ptr=(uint8_t *)header; ptr<endptr; ptr++)
      checksum += *ptr;
    header->checksum = checksum;

    {
      boost::mutex::scoped_lock lock(m_mutex);

      if ((error = m_fs->append(m_fd, header, sizeof(CommitLogHeaderT))) != Error::OK) {
	LOG_VA_ERROR("Problem appending %d bytes to commit log file '%s' - %s", sizeof(CommitLogHeaderT), m_log_file.c_str(), Error::get_text(error));
	return error;
      }

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

  }

  return Error::OK;
}


/** This method links an external log into this log by first rolling the log.
 * The current log file is closed and a new one is opened.  The linked to log
 * directory is written as the first log entry in the newly opened file.  Link
 * entries have the Type::LINK bit set in the flags field of the header.
 */
int CommitLog::link_log(const char *table_name, const char *log_dir, uint64_t timestamp) {
  uint8_t *ptr, *endptr;
  uint16_t checksum = 0;
  int error;
  CommitLogFileInfoT fileInfo;
  CommitLogHeaderT *header;

  /**
   * Roll the log
   */

  // Write trailing empty block
  header = new CommitLogHeaderT[1];
  memcpy(header->marker, "-BLOCK--", 8);
  header->checksum = 0;
  header->timestamp = m_last_timestamp;
  header->length = sizeof(CommitLogHeaderT);
  header->flags = 0;

  checksum = 0;
  endptr = (uint8_t *)&header[1];
  for (ptr=(uint8_t *)header; ptr<endptr; ptr++)
    checksum += *ptr;
  header->checksum = checksum;

  {
    boost::mutex::scoped_lock lock(m_mutex);
    
    if ((error = m_fs->append(m_fd, header, sizeof(CommitLogHeaderT))) != Error::OK) {
      LOG_VA_ERROR("Problem appending %d bytes to commit log file '%s' - %s", sizeof(CommitLogHeaderT), m_log_file.c_str(), Error::get_text(error));
      return error;
    }

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

  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  uint32_t totalLen = sizeof(CommitLogHeaderT) + strlen(table_name) + 1 + strlen(log_dir) + 1;
  uint8_t *block = new uint8_t [ totalLen ];
  header = (CommitLogHeaderT *)block;

  // marker
  memcpy(header->marker, "-BLOCK--", 8);
  header->timestamp = timestamp;
  header->length = totalLen;
  header->checksum = 0;
  header->flags = LINK;

  // table name
  ptr = block + sizeof(CommitLogHeaderT);
  strcpy((char *)ptr, table_name);
  ptr += strlen((char *)ptr) + 1;

  // external log
  strcpy((char *)ptr, log_dir);
  ptr += strlen((char *)ptr) + 1;

  // compute checksum
  endptr = block + totalLen;
  for (ptr=block; ptr<endptr; ptr++)
    checksum += *ptr;
  header->checksum = checksum;

  // kick off log write (protected by lock)
  {
    boost::mutex::scoped_lock lock(m_mutex);
    if ((error = m_fs->append(m_fd, block, totalLen, &sync_handler)) != Error::OK)
      return error;
    if ((error = m_fs->flush(m_fd, &sync_handler)) != Error::OK)
      return error;
    m_last_timestamp = timestamp;
    m_cur_log_length += totalLen;
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

  // Add external log to list of files/dirs to be garbage collected
  fileInfo.timestamp = m_last_timestamp;
  fileInfo.fname = log_dir;
  m_file_info_queue.push(fileInfo);

  return Error::OK;
}



/**
 * 
 */
int CommitLog::close(uint64_t timestamp) {
  CommitLogHeaderT *header = new CommitLogHeaderT[1];
  int error;
  uint8_t *ptr, *endptr;
  uint16_t checksum = 0;

  memcpy(header->marker, "-BLOCK--", 8);
  header->timestamp = timestamp;
  header->length = sizeof(CommitLogHeaderT);
  header->checksum = 0;
  header->flags = 0;

  checksum = 0;
  endptr = (uint8_t *)&header[1];
  for (ptr=(uint8_t *)header; ptr<endptr; ptr++)
    checksum += *ptr;
  header->checksum = checksum;

  {
    boost::mutex::scoped_lock lock(m_mutex);

    if ((error = m_fs->append(m_fd, header, sizeof(CommitLogHeaderT))) != Error::OK) {
      LOG_VA_ERROR("Problem appending %d bytes to commit log file '%s' - %s", sizeof(CommitLogHeaderT), m_log_file.c_str(), Error::get_text(error));
      return error;
    }

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
    if (fileInfo.timestamp < timestamp) {
      // should do something on error, but for now, just move on
      if ((error = m_fs->remove(fileInfo.fname)) == Error::OK)
	m_file_info_queue.pop();
    }
    else
      break;
  }
  return Error::OK;
}
