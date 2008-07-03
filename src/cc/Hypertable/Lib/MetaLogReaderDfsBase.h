/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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

#ifndef HYPERTABLE_METALOG_READER_DFS_BASE_H
#define HYPERTABLE_METALOG_READER_DFS_BASE_H

#include "Common/String.h"
#include "MetaLogReader.h"

namespace Hypertable {

class Filesystem;

class MetaLogReaderDfsBase : public MetaLogReader {
public:
  MetaLogReaderDfsBase(Filesystem *fs, const String &path);

  virtual ScanEntry *next(ScanEntry &);

  Filesystem &fs() { return *m_fs; }
  int fd() { return m_fd; }
  const String &path() const { return m_path; }
  size_t &pos() { return m_cur; }
  size_t size() const { return m_file_size; }

private:
  int m_fd;
  Filesystem *m_fs;
  String m_path;
  size_t m_cur, m_file_size;
};

} // namespace Hypertable

#endif // HYPERTABLE_METALOG_READER_DFS_BASE_H
