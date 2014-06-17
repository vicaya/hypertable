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

bool CommitLogBlockStream::ms_assert_on_error = true;


CommitLogBlockStream::CommitLogBlockStream(FilesystemPtr &fs)
  : m_fs(fs), m_fd(-1), m_cur_offset(0), m_file_length(0),
    m_block_buffer(BlockCompressionHeaderCommitLog::LENGTH) {
}


CommitLogBlockStream::CommitLogBlockStream(FilesystemPtr &fs,
    const String &log_dir, const String &fragment)
  : m_fs(fs), m_fd(-1), m_cur_offset(0), m_file_length(0),
    m_block_buffer(BlockCompressionHeaderCommitLog::LENGTH) {
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
  m_log_dir = log_dir;
  m_cur_offset = 0;
  m_file_length = m_fs->length(m_fname);
  m_fd = m_fs->open_buffered(m_fname, Filesystem::OPEN_FLAG_DIRECTIO, READAHEAD_BUFFER_SIZE, 2);
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


bool
CommitLogBlockStream::next(CommitLogBlockInfo *infop,
                           BlockCompressionHeaderCommitLog *header) {
  uint32_t nread;

  assert(m_fd != -1);

  if (m_cur_offset >= m_file_length)
    return false;

  memset(infop, 0, sizeof(CommitLogBlockInfo));
  infop->log_dir = m_log_dir.c_str();
  infop->file_fragment = m_fragment.c_str();
  infop->start_offset = m_cur_offset;

  if ((infop->error = load_next_valid_header(header)) != Error::OK) {
    infop->end_offset = m_cur_offset;
    return false;
  }

  m_cur_offset += BlockCompressionHeaderCommitLog::LENGTH;

  // check for truncation
  if ((m_file_length - m_cur_offset) < header->get_data_zlength()) {
    HT_WARNF("Commit log fragment '%s' truncated (entry start position %llu)",
             m_fname.c_str(), (Llu)(m_cur_offset-BlockCompressionHeaderCommitLog::LENGTH));
    infop->end_offset = m_file_length;
    infop->error = Error::RANGESERVER_TRUNCATED_COMMIT_LOG;
    m_cur_offset = m_file_length;
    return true;
  }

  m_block_buffer.ensure(BlockCompressionHeaderCommitLog::LENGTH
                        + header->get_data_zlength());

  nread = m_fs->read(m_fd, m_block_buffer.ptr, header->get_data_zlength());

  if (nread != header->get_data_zlength()) {
    HT_WARNF("Commit log fragment '%s' truncated (entry start position %llu)",
             m_fname.c_str(), (Llu)(m_cur_offset-BlockCompressionHeaderCommitLog::LENGTH));
    infop->end_offset = m_file_length;
    infop->error = Error::RANGESERVER_TRUNCATED_COMMIT_LOG;
    m_cur_offset = m_file_length;
    return true;
  }

  m_block_buffer.ptr += nread;
  m_cur_offset += nread;
  infop->end_offset = m_cur_offset;
  infop->block_ptr = m_block_buffer.base;
  infop->block_len = m_block_buffer.fill();

  return true;
}


int
CommitLogBlockStream::load_next_valid_header(
    BlockCompressionHeaderCommitLog *header) {
  size_t remaining = BlockCompressionHeaderCommitLog::LENGTH;
  try {
    size_t nread = 0;
    size_t toread = BlockCompressionHeaderCommitLog::LENGTH;

    m_block_buffer.ptr = m_block_buffer.base;

    while ((nread = m_fs->read(m_fd, m_block_buffer.ptr, toread)) < toread) {
      toread -= nread;
      m_block_buffer.ptr += nread;
    }

    m_block_buffer.ptr = m_block_buffer.base;
    header->decode((const uint8_t **)&m_block_buffer.ptr, &remaining);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    if (ms_assert_on_error)
      HT_ABORT;
    return e.code();
  }
  return Error::OK;
}
