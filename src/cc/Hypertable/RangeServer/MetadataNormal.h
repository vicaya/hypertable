/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_METADATANORMAL_H
#define HYPERTABLE_METADATANORMAL_H

#include <string>

#include "Hypertable/Lib/TableScanner.h"
#include "Hypertable/Lib/Types.h"

#include "Metadata.h"

namespace Hypertable {
  class MetadataNormal : public Metadata {

  public:
    MetadataNormal(TableIdentifierT &identifier, std::string &end_row);
    virtual ~MetadataNormal();
    virtual void reset_files_scan();
    virtual bool get_next_files(std::string &ag_name, std::string &files);
    virtual void write_files(std::string &ag_name, std::string &files);

  private:
    TableScannerPtr   m_files_scanner_ptr;
    TableIdentifierT  m_identifier;
    std::string       m_metadata_key;
  };
}

#endif // HYPERTABLE_METADATANORMAL_H

