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

#ifndef HYPERTABLE_METALOGREADER_H
#define HYPERTABLE_METALOGREADER_H

#include "Common/Filesystem.h"
#include "Common/ReferenceCount.h"

#include <map>
#include <vector>

#include "MetaLogDefinition.h"


namespace Hypertable {

  namespace MetaLog {

    class Reader : public ReferenceCount {

    public:
      Reader(FilesystemPtr &fs, DefinitionPtr &definition);
      Reader(FilesystemPtr &fs, DefinitionPtr &definition, const String &path);
      void get_entities(std::vector<EntityPtr> &entities);
      void get_all_entities(std::vector<EntityPtr> &entities);
      void reload();
      int32_t next_file_number() { return m_next_filenum; }
      bool load_file(const String &fname);

    private:

      void read_header(int fd);

      FilesystemPtr m_fs;
      MetaLog::DefinitionPtr m_definition;
      String m_path;
      int32_t m_next_filenum;
      std::vector<int32_t> m_file_nums;
      std::map<EntityHeader, EntityPtr> m_entity_map;
      std::vector<EntityPtr> m_entities;
      size_t m_cur_offset;

    };
    typedef intrusive_ptr<Reader> ReaderPtr;
  }
}

#endif // HYPERTABLE_METALOGREADER_H
