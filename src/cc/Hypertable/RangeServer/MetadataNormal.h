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

#ifndef HYPERTABLE_METADATANORMAL_H
#define HYPERTABLE_METADATANORMAL_H

#include <string>

#include "Common/HashMap.h"

#include "Hypertable/Lib/TableScanner.h"
#include "Hypertable/Lib/Types.h"

#include "Metadata.h"

namespace Hypertable {
  class MetadataNormal : public Metadata {

  public:
    MetadataNormal(const TableIdentifier *identifier, const String &end_row);
    virtual ~MetadataNormal();
    virtual void reset_files_scan();
    virtual bool get_next_files(String &ag_name, String &files, uint32_t *nextcsidp);
    virtual void write_files(const String &ag_name, const String &files);
    virtual void write_files(const String &ag_name, const String &files, uint32_t nextcsid);

  private:

    class AgMetadata {
    public:
      AgMetadata() : nextcsid(0) { }
      String files;
      uint32_t nextcsid;
    };
    typedef hash_map<String, AgMetadata> AgMetadataMap;
    
    String          m_metadata_key;
    AgMetadataMap   m_ag_map;
    AgMetadataMap::iterator  m_ag_map_iter;
  };
}

#endif // HYPERTABLE_METADATANORMAL_H

