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
CommitLogBlockStream::CommitLogBlockStream(Filesystem *fs) : m_fs(fs), m_fd(-1), m_cur_offset(0), m_file_length(0),m_block_buffer(BlockCompressionHeaderCommitLog::LENGTH) {
}

CommitLogBlockStream::CommitLogBlockStream(Filesystem *fs, const String &log_dir, const String &fragment) : m_fs(fs), m_fd(-1), m_cur_offset(0), m_file_length(0), m_block_buffer(BlockCompressionHeaderCommitLog::LENGTH) {
  load(log_dir, fragment);
}

CommitLogBlockStream::~CommitLogBlockStream() {
  close();
}
void CommitLogBlockStream::load(const String &log_dir, const String &fragment) {
  if (m_fd != -1)
    close();
  m_fragment = fragment;
  m_fname = log_dir + fragment;
  m_cur_offset = 0;
  m_file_length = m_fs->length(m_fname);
  m_fd = m_fs->open_buffered(m_fname, READAHEAD_BUFFER_SIZE, 2);
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

bool CommitLogBlockStream::next(CommitLogBlockInfo *infop, BlockCompressionHeaderCommitLog *header) {
  uint32_t nread;

  assert(m_fd != -1);

  if (m_cur_offset >= m_file_length)
    return false;

  memset(infop, 0, sizeof(CommitLogBlockInfo));
  infop->file_fragment = m_fragment.c_str();
  infop->start_offset = m_cur_offset;

  if ((infop->error = load_next_valid_header(header)) != Error::OK) {
    infop->end_offset = m_cur_offset;
    return true;
  }

  m_cur_offset += BlockCompressionHeaderCommitLog::LENGTH;

  // check for truncation
  if ((m_file_length - m_cur_offset) < header->get_data_zlength()) {
    infop->end_offset = m_file_length;
    infop->error = Error::RANGESERVER_TRUNCATED_COMMIT_LOG;
    m_cur_offset = m_file_length;
    return true;
  }

  m_block_buffer.ensure(BlockCompressionHeaderCommitLog::LENGTH + header->get_data_zlength());

  nread = m_fs->read(m_fd, m_block_buffer.ptr, header->get_data_zlength());

  HT_EXPECT(nread == header->get_data_zlength(), Error::FAILED_EXPECTATION);

  m_block_buffer.ptr += nread;
  m_cur_offset += nread;
  infop->end_offset = m_cur_offset;
  infop->block_ptr = m_block_buffer.base;
  infop->block_len = m_block_buffer.fill();

  return true;
}



/**
 */
int CommitLogBlockStream::load_next_valid_header(BlockCompressionHeaderCommitLog *header) {
  uint32_t nread;
  size_t remaining = BlockCompressionHeaderCommitLog::LENGTH;
  try {
    nread = m_fs->read(m_fd, m_block_buffer.base,
                       BlockCompressionHeaderCommitLog::LENGTH);
    HT_EXPECT(nread == BlockCompressionHeaderCommitLog::LENGTH, -1);

    m_block_buffer.ptr = m_block_buffer.base;
    header->decode((const uint8_t **)&m_block_buffer.ptr, &remaining);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return e.code();
  }
  return Error::OK;
}

