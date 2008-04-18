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
#include "Common/Error.h"
#include "Common/Logger.h"

#include "CommitLogBlockStream.h"

using namespace Hypertable;

namespace {
  const uint32_t READAHEAD_BUFFER_SIZE = 131072;
}

/**
 */
CommitLogBlockStream::CommitLogBlockStream(Filesystem *fs) : m_fs(fs), m_fd(-1), m_cur_offset(0), m_file_length(0), m_got_header(false), m_block_buffer(BlockCompressionHeaderCommitLog::LENGTH) {
}

CommitLogBlockStream::CommitLogBlockStream(Filesystem *fs, const String &fname) : m_fs(fs), m_fd(-1), m_cur_offset(0), m_file_length(0), m_got_header(false), m_block_buffer(BlockCompressionHeaderCommitLog::LENGTH) {
  load(fname);
}

CommitLogBlockStream::~CommitLogBlockStream() {
  close();
}

void CommitLogBlockStream::load(const String &fname) {
  if (m_fd != -1)
    close();
  m_fname = fname;
  m_cur_offset = 0;
  m_file_length = m_fs->length(fname);
  m_fd = m_fs->open_buffered(fname, READAHEAD_BUFFER_SIZE, 2);
  m_got_header = false;
}  

void CommitLogBlockStream::close() {
  if (m_fd != -1) {
    try {
      m_fs->close(m_fd);
    }
    catch (Hypertable::Exception &e) {
      HT_ERRORF("Problem closing file %s - %s (%s)",
		m_fname.c_str(), e.what(), Error::get_text(e.code()));
    }
    m_fd = -1;
  }
}

bool CommitLogBlockStream::next(uint8_t **blockp, size_t *lenp, BlockInfo *infop) {
  uint32_t nread;

  assert(m_fd != -1);

  if (m_cur_offset >= m_file_length)
    return false;

  memset(infop, 0, sizeof(BlockInfo));
  infop->start_offset = m_cur_offset;

  if (!m_got_header) {
    if ((infop->error = load_next_valid_header()) != Error::OK) {
      infop->end_offset = m_cur_offset;
      *blockp = 0;
      *lenp = 0;
      return true;
    }
  }

  m_cur_offset += BlockCompressionHeaderCommitLog::LENGTH;

  // check for truncation
  if ((m_file_length - m_cur_offset) < m_header.get_data_zlength()) {
    infop->end_offset = m_file_length;
    infop->error = Error::RANGESERVER_TRUNCATED_COMMIT_LOG;
    m_cur_offset = m_file_length;
    *blockp = 0;
    *lenp = 0;
    return true;
  }

  m_block_buffer.ensure(BlockCompressionHeaderCommitLog::LENGTH + m_header.get_data_zlength());

  nread = m_fs->read(m_fd, m_block_buffer.ptr, m_header.get_data_zlength());

  HT_EXPECT(nread == m_header.get_data_zlength(), Error::FAILED_EXPECTATION);

  m_block_buffer.ptr += nread;
  m_cur_offset += nread;
  infop->end_offset = m_cur_offset;

  *blockp = m_block_buffer.buf;
  *lenp = m_block_buffer.fill();

  return true;
}



/**
 */
int CommitLogBlockStream::load_next_valid_header() {
  uint32_t nread;
  size_t remaining = BlockCompressionHeaderCommitLog::LENGTH;

  nread = m_fs->read(m_fd, m_block_buffer.buf, BlockCompressionHeaderCommitLog::LENGTH);

  HT_EXPECT(nread == BlockCompressionHeaderCommitLog::LENGTH, Error::FAILED_EXPECTATION);

  m_block_buffer.ptr = m_block_buffer.buf;

  HT_EXPECT(m_header.decode(&m_block_buffer.ptr, &remaining) == Error::OK, Error::FAILED_EXPECTATION);

  m_got_header = false;

  return Error::OK;
}

