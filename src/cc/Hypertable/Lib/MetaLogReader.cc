/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
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
#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/ScopeGuard.h"
#include "Common/Serialization.h"
#include "Common/StringExt.h"

#include <algorithm>
#include <cstdio>

#include <boost/algorithm/string.hpp>
#include <boost/shared_ptr.hpp>

#include "MetaLog.h"
#include "MetaLogReader.h"

using namespace Hypertable;
using namespace Hypertable::MetaLog;

Reader::Reader(FilesystemPtr &fs, DefinitionPtr &definition) :
  m_fs(fs), m_definition(definition) {
}


Reader::Reader(FilesystemPtr &fs, DefinitionPtr &definition, const String &path) :
  m_fs(fs), m_definition(definition) {

  // Setup DFS path name
  m_path = path;
  boost::trim_right_if(m_path, boost::is_any_of("/"));

  reload();
}

void Reader::get_entities(std::vector<EntityPtr> &entities) {
  std::map<EntityHeader, EntityPtr>::iterator iter;
  for (iter = m_entity_map.begin(); iter != m_entity_map.end(); ++iter)
    entities.push_back(iter->second);
}

void Reader::get_all_entities(std::vector<EntityPtr> &entities) {
  entities = m_entities;
}


void Reader::reload() {

  scan_log_directory(m_fs, m_path, m_file_nums, &m_next_filenum);

  std::sort(m_file_nums.begin(), m_file_nums.end());

  while (!m_file_nums.empty()) {
    String fname = m_path + "/" + m_file_nums.back();

    if (!load_file(fname)) {
      String badname = m_path + "/" + m_file_nums.back() + ".bad";
      HT_INFOF("Moving problematic %s log file %s to %s",
               m_definition->name(), fname.c_str(), badname.c_str());
      try {
        m_fs->rename(fname, badname);
      }
      catch (Exception &e) {
        HT_ERROR_OUT << e << HT_END;
      }
      m_file_nums.resize( m_file_nums.size() - 1 );
      continue;
    }
    break;
  }
}


namespace {
  const uint32_t READAHEAD_BUFSZ = 1024 * 1024;
  const uint32_t OUTSTANDING_READS = 1;
  void close_descriptor(FilesystemPtr fs, int *fdp) {
    try {
      fs->close(*fdp, (DispatchHandler *)0);
    }
    catch (Exception &e) {
      HT_ERRORF("Problem closing MetaLog: %s - %s",
                Error::get_text(e.code()), e.what());
    }
  }
}

bool Reader::load_file(const String &fname) {
  size_t file_length = m_fs->length(fname);
  int fd = m_fs->open_buffered(fname, 0, READAHEAD_BUFSZ, OUTSTANDING_READS);
  bool found_recover_entry = false;

  m_entity_map.clear();

  m_cur_offset = 0;

  HT_ON_SCOPE_EXIT(&close_descriptor, m_fs, &fd);

  read_header(fd);

  try {
    size_t remaining;
    EntityHeader header;
    DynamicBuffer buf;

    buf.reserve(EntityHeader::LENGTH);

    while (m_cur_offset < file_length) {

      buf.clear();

      size_t nread = m_fs->read(fd, buf.base, EntityHeader::LENGTH);

      if (nread != EntityHeader::LENGTH)
        HT_THROW(Error::METALOG_ENTRY_TRUNCATED, "reading entity header");

      remaining = EntityHeader::LENGTH;
      const uint8_t *ptr = buf.base;
      header.decode(&ptr, &remaining);

      m_cur_offset += nread;

      if (header.type == EntityType::RECOVER) {
        found_recover_entry = true;
        continue;
      }
      else if (header.flags & EntityHeader::FLAG_REMOVE) {
        std::map<EntityHeader, EntityPtr>::iterator iter = m_entity_map.find(header);
        if (iter != m_entity_map.end())
          m_entity_map.erase(iter);
        continue;
      }

      EntityPtr entity = m_definition->create(header);

      buf.clear();
      buf.ensure(header.length);

      nread = m_fs->read(fd, buf.base, header.length);

      if (nread != (size_t)header.length)
        HT_THROW(Error::METALOG_ENTRY_TRUNCATED, "reading entity payload");

      m_cur_offset += nread;

      remaining = header.length;
      ptr = buf.base;
      entity->decode(&ptr, &remaining);

      // verify checksum
      int32_t computed_checksum = fletcher32(buf.base, header.length);
      if (header.checksum != computed_checksum)
        HT_THROWF(Error::METALOG_CHECKSUM_MISMATCH,
                  "MetaLog entry checksum mismatch header=%d, computed=%d",
                  header.checksum, computed_checksum);

      m_entity_map[header] = entity;
      m_entities.push_back(entity);

    }

  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Error reading metalog file: %s: read %llu/%llu",
               fname.c_str(), (Llu)m_cur_offset, (Llu)file_length);
  }

  return found_recover_entry;
}


void Reader::read_header(int fd) {
  MetaLog::Header header;
  uint8_t buf[Header::LENGTH];
  const uint8_t *ptr = buf;
  size_t remaining = Header::LENGTH;

  size_t nread = m_fs->read(fd, buf, Header::LENGTH);

  if (nread != Header::LENGTH)
    HT_THROWF(Error::METALOG_BAD_HEADER,
              "Short read of %s header (expected %d, got %d)",
              m_definition->name(), (int)Header::LENGTH, (int)nread);

  m_cur_offset += nread;

  header.decode(&ptr, &remaining);

  if (strcmp(header.name, m_definition->name()))
    HT_THROWF(Error::METALOG_BAD_HEADER, "Wrong name in %s header ('%s' != '%s')",
              m_definition->name(), header.name, m_definition->name());

  if (header.version != m_definition->version())
    HT_THROWF(Error::METALOG_VERSION_MISMATCH,
              "Incompatible version in %s header (expected %d, got %d)",
              m_definition->name(), m_definition->version(), header.version);
}
