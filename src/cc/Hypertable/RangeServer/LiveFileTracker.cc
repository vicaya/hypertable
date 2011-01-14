/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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
#include "Common/FailureInducer.h"

#include "Hypertable/Lib/Key.h"

#include "Global.h"
#include "LiveFileTracker.h"
#include "MetadataNormal.h"
#include "MetadataRoot.h"

using namespace Hypertable;

LiveFileTracker::LiveFileTracker(const TableIdentifier *identifier,
                                 SchemaPtr &schema_ptr,
                                 const RangeSpec *range,
                                 const String &ag_name) :
  m_identifier(*identifier), m_schema_ptr(schema_ptr),
  m_start_row(range->start_row), m_end_row(range->end_row),
  m_ag_name(ag_name), m_need_update(false), m_is_root(false) {

  m_is_root = (m_identifier.is_metadata() && *range->start_row == 0
               && !strcmp(range->end_row, Key::END_ROOT_ROW));

  m_file_basename = Global::toplevel_dir + "/tables/";

}

void LiveFileTracker::update_live(const String &add, std::vector<String> &deletes) {
  ScopedLock lock(m_mutex);
  for (size_t i=0; i<deletes.size(); i++)
    m_live.erase(strip_basename(deletes[i]));
  if (add != "")
    m_live.insert(strip_basename(add));
  m_need_update = true;
}


void LiveFileTracker::add_references(const std::vector<String> &filev) {
  ScopedLock lock(m_mutex);
  FileRefCountMap::iterator iter;
  for (size_t i=0; i<filev.size(); i++) {
    iter = m_referenced.find(filev[i]);
    if (iter == m_referenced.end())
      m_referenced[filev[i]] = 1;
    else
      (*iter).second++;
  }
}

/**
 * If the file removed is one that has been written into the
 * 'Files' column as 'blocked', then the m_need_update flag
 * is set to true, indicating the the column needs updating.
 */
void LiveFileTracker::remove_references(const std::vector<String> &filev) {
  ScopedLock lock(m_mutex);
  FileRefCountMap::iterator iter;
  for (size_t i=0; i<filev.size(); i++) {
    iter = m_referenced.find(filev[i]);
    HT_ASSERT(iter != m_referenced.end());
    if (--(*iter).second == 0) {
      m_referenced.erase(iter);
      if (m_blocked.count(filev[i]) > 0)
        m_need_update = true;
    }
  }
}


void LiveFileTracker::update_files_column() {

  if (!m_need_update)
    return;

  String file_list;
  String printable_list;
  String end_row;
  int retry_count = 0;

  try_again:

  file_list.clear();
  end_row.clear();

  m_mutex.lock();

  foreach(const String &file, m_live) {
    file_list += file + ";\n";
    printable_list += file + "; ";
  }

  m_blocked.clear();
  foreach(const FileRefCountMap::value_type &v, m_referenced) {
    if (m_live.count(v.first) == 0) {
      file_list += format("#%s;\n", v.first.c_str());
      printable_list += String("#") + v.first + "; ";
      m_blocked.insert(v.first);
    }
  }

  end_row = m_end_row;

  m_update_mutex.lock();
  m_mutex.unlock();

  try {
    if (m_is_root) {
      MetadataRoot metadata(m_schema_ptr);
      metadata.write_files(m_ag_name, file_list);
    }
    else {
      MetadataNormal metadata(&m_identifier, end_row);
      metadata.write_files(m_ag_name, file_list);
    }
    HT_MAYBE_FAIL("LiveFileTracker-update_files_column");
  }
  catch (Hypertable::Exception &e) {
    m_update_mutex.unlock();
    HT_ERROR_OUT <<"Problem updating 'Files' column of METADATA, retry count=" << retry_count
                 << " : " << e << HT_END;
    if (retry_count < 6) {
      ++retry_count;
      poll(0, 0, 15000);
      goto try_again;
    }
      HT_THROW2(e.code(), e, "Problem updating 'Files' column of METADATA: ");
  }

  m_need_update = false;
  m_update_mutex.unlock();

}


void LiveFileTracker::get_file_list(String &file_list, bool include_blocked) {
  ScopedLock lock(m_mutex);

  file_list = "";

  foreach(const String &file, m_live)
    file_list += file + ";\n";

  if (include_blocked) {
    m_blocked.clear();
    foreach(const FileRefCountMap::value_type &v, m_referenced) {
      if (m_live.count(v.first) == 0) {
        file_list += format("#%s;\n", v.first.c_str());
        m_blocked.insert(v.first);
      }
    }
  }

}

String LiveFileTracker::strip_basename(const String &fname) {
  size_t basename_len = m_file_basename.length();
  HT_ASSERT(!strncmp(fname.c_str(), m_file_basename.c_str(), basename_len));
  return fname.substr(basename_len);
}
