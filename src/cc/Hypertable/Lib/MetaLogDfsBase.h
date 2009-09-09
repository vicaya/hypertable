/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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

#ifndef HYPERTABLE_METALOG_DFS_BASE_H
#define HYPERTABLE_METALOG_DFS_BASE_H

#include "Common/String.h"
#include "Common/Mutex.h"
#include "MetaLog.h"

namespace Hypertable {

class Filesystem;

class MetaLogDfsBase : public MetaLog {
public:
  MetaLogDfsBase(Filesystem *fs, const String &path);
  virtual ~MetaLogDfsBase() { close(); }

  virtual void write(MetaLogEntry *entry);
  virtual void close();

  Filesystem &fs() { return *m_fs; }

  void find_or_create_file();
  void get_filename();

protected:
  virtual int create(const String &path, bool overwrite = false);
  virtual void serialize_entry(MetaLogEntry *entry, DynamicBuffer &buf);
  virtual void write_unlocked(DynamicBuffer &);

  int fd() { return m_fd; }
  void fd(int f) { m_fd = f; }
  int32_t fileno() { return m_fileno; }
  void fileno(int32_t n) { m_fileno = n; }
  String &filename() { return m_filename; }
  void filename(const String &f) { m_filename = f; }
  const String& path() const { return m_path; }
  Mutex &mutex() { return m_mutex; }

private:
  Mutex m_mutex;
  int m_fd;
  Filesystem *m_fs;
  DynamicBuffer m_buf;
  String m_path;
  int32_t m_fileno;
  String m_filename;
};

} // namespace Hypertable

#endif // HYPERTABLE_METALOG_DFS_BASE_H
