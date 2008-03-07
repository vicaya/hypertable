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

#ifndef HYPERTABLE_TABLEINFOMAP_H
#define HYPERTABLE_TABLEINFOMAP_H

#include <string>

#include <boost/intrusive_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "TableInfo.h"

namespace Hypertable {

  /** 
   * Provides a mapping from table name to TableInfo object.
   */
  class TableInfoMap : public ReferenceCount {
  public:
    TableInfoMap() { return; }
    virtual ~TableInfoMap();

    bool get(std::string name, TableInfoPtr &info);
    void set(std::string name, TableInfoPtr &info);
    bool remove(std::string name, TableInfoPtr &info);
    void get_all(std::vector<TableInfoPtr> &tv);

  private:
    typedef __gnu_cxx::hash_map<string, TableInfoPtr> TableInfoMapT;

    boost::mutex  m_mutex;
    TableInfoMapT m_map;
  };
  typedef boost::intrusive_ptr<TableInfoMap> TableInfoMapPtr;

}

#endif // HYPERTABLE_TABLEINFOMAP_H
