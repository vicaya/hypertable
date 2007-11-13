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

#include <string>
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
#include "Common/Logger.h"

#include "CommitLog.h"
#include "CommitLogReader.h"

using namespace Hypertable;
using namespace std;

namespace {
  const uint32_t READAHEAD_BUFFER_SIZE = 131072;
}

/**
 *
 */
CommitLogReader::CommitLogReader(Filesystem *fs, std::string &logDir) : m_fs(fs), m_log_dir(logDir), m_fd(-1), m_block_buffer(sizeof(CommitLogHeaderT)) {
  LogFileInfoT fileInfo;
  int error;
  int32_t fd;
  CommitLogHeaderT  header;
  vector<string> listing;
  int64_t flen;
  uint32_t nread;

  LOG_VA_INFO("LogDir = %s", logDir.c_str());

  if (m_log_dir.find('/', m_log_dir.length()-1) == string::npos)
    m_log_dir += "/";

  if ((error = m_fs->readdir(m_log_dir, listing)) != Error::OK) {
    LOG_VA_ERROR("Problem reading directory '%s' - %s", m_log_dir.c_str(), Error::get_text(error));
    exit(1);
  }

  m_log_file_info.clear();

  for (size_t i=0; i<listing.size(); i++) {
    char *endptr;
    long num = strtol(listing[i].c_str(), &endptr, 10);
    if (*endptr != 0) {
      LOG_VA_WARN("Invalid file '%s' found in commit log directory '%s'", listing[i].c_str(), m_log_dir.c_str());
    }
    else {
      fileInfo.num = (uint32_t)num;
      fileInfo.fname = m_log_dir + listing[i];
      m_log_file_info.push_back(fileInfo);
    }
  }

  sort(m_log_file_info.begin(), m_log_file_info.end());

  for (size_t i=0; i<m_log_file_info.size(); i++) {

    m_log_file_info[i].timestamp = 0;

    if ((error = m_fs->length(m_log_file_info[i].fname, &flen)) != Error::OK) {
      LOG_VA_ERROR("Problem trying to determine length of commit log file '%s' - %s", m_log_file_info[i].fname.c_str(), strerror(errno));
      exit(1);
    }

    if (flen < sizeof(CommitLogHeaderT))
      continue;

    if ((error = m_fs->open(m_log_file_info[i].fname, &fd)) != Error::OK) {
      LOG_VA_ERROR("Problem opening commit log file '%s' - %s", m_log_file_info[i].fname.c_str(), strerror(errno));
      exit(1);
    }

    if ((error = m_fs->pread(fd, flen-sizeof(CommitLogHeaderT), sizeof(CommitLogHeaderT), (uint8_t *)&header, &nread)) != Error::OK ||
	nread != sizeof(CommitLogHeaderT)) {
      LOG_VA_ERROR("Problem reading trailing header in commit log file '%s' - %s", m_log_file_info[i].fname.c_str(), strerror(errno));
      exit(1);
    }

    if ((error = m_fs->close(fd)) != Error::OK) {
      LOG_VA_ERROR("Problem closing commit log file '%s' - %s", m_log_file_info[i].fname.c_str(), strerror(errno));
      exit(1);
    }

    if (!strncmp(header.marker, "-BLOCK--", 8))
      m_log_file_info[i].timestamp = header.timestamp;

    //cout << m_log_file_info[i].num << ":  " << m_log_file_info[i].fname << " " << m_log_file_info[i].timestamp << endl;
  }

}



void CommitLogReader::initialize_read(uint64_t timestamp) {
  m_cutoff_time = timestamp;
  m_cur_log_offset = 0;
  m_fd = -1;
}



bool CommitLogReader::next_block(CommitLogHeaderT **blockp) {
  bool done = false;
  size_t toread;
  uint32_t nread;
  int error;

  while (!done) {

    if (m_fd == -1) {

      while (m_cur_log_offset < m_log_file_info.size()) {
	if (m_log_file_info[m_cur_log_offset].timestamp == 0 ||
	    m_log_file_info[m_cur_log_offset].timestamp > m_cutoff_time)
	  break;
	m_cur_log_offset++;
      }

      if (m_cur_log_offset == m_log_file_info.size())
	return false;

      if ((error = m_fs->open_buffered(m_log_file_info[m_cur_log_offset].fname, READAHEAD_BUFFER_SIZE, &m_fd)) != Error::OK) {
	LOG_VA_ERROR("Problem trying to open commit log file '%s' - %s", m_log_file_info[m_cur_log_offset].fname.c_str(), Error::get_text(error));
	return false;
      }
    }

    m_block_buffer.ptr = m_block_buffer.buf;

    if ((error = m_fs->read(m_fd, sizeof(CommitLogHeaderT), m_block_buffer.ptr, &nread)) != Error::OK) {
      LOG_VA_ERROR("Problem reading header from commit log file '%s' - %s", m_log_file_info[m_cur_log_offset].fname.c_str(), Error::get_text(error));
      return false;
    }
    
    if (nread != sizeof(CommitLogHeaderT)) {
      if ((error = m_fs->close(m_fd)) != Error::OK) {
	LOG_VA_ERROR("Problem closing commit log file '%s' - %s", m_log_file_info[m_cur_log_offset].fname.c_str(), Error::get_text(error));
	return false;
      }
      m_fd = -1;
      m_cur_log_offset++;
    }
    else {
      CommitLogHeaderT *header = (CommitLogHeaderT *)m_block_buffer.buf;
      if (header->length == sizeof(CommitLogHeaderT))
	continue;
      m_block_buffer.ptr += sizeof(CommitLogHeaderT);
      // TODO: sanity check this header
      toread = header->length-sizeof(CommitLogHeaderT);
      m_block_buffer.ensure(toread+sizeof(CommitLogHeaderT));

      if ((error = m_fs->read(m_fd, toread, m_block_buffer.ptr, &nread)) != Error::OK) {
	LOG_VA_ERROR("Problem reading commit block from commit log file '%s' - %s", m_log_file_info[m_cur_log_offset].fname.c_str(), Error::get_text(error));
	return false;
      }
      
      if (nread != toread) {
	LOG_VA_ERROR("Short read of commit log block '%s'", m_log_file_info[m_cur_log_offset].fname.c_str());
	if ((error = m_fs->close(m_fd)) != Error::OK) {
	  LOG_VA_ERROR("Problem closing commit log file '%s' - %s", m_log_file_info[m_cur_log_offset].fname.c_str(), Error::get_text(error));
	  return false;
	}
	m_fd = -1;
	return false;
      }
      *blockp = (CommitLogHeaderT *)m_block_buffer.buf;
      return true;
    }
  }

  return false;
  
}
