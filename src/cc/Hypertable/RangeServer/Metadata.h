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

#ifndef HYPERTABLE_METADATA_H
#define HYPERTABLE_METADATA_H

namespace Hypertable {

  class Metadata {
  public:
    virtual ~Metadata() { return; }
    virtual void reset_files_scan() = 0;
    virtual bool get_next_files(std::string &ag_name, std::string &files) = 0;
    virtual void write_files(std::string &ag_name, std::string &files) = 0;
  };

}

#endif // HYPERTABLE_METADATA_H
