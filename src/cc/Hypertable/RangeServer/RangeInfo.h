/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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
#ifndef HYPERTABLE_TABLETINFO_H
#define HYPERTABLE_TABLETINFO_H

#include <set>
#include <string>
#include <vector>
#include <iostream>

#include <boost/thread/mutex.hpp>

#include "Common/ReferenceCount.h"

namespace Hypertable {

  class RangeInfo : public ReferenceCount {
  public:
    RangeInfo() : m_mutex(), m_table_name(""), m_start_row(""), m_end_row(""), m_log_dir(""), cellStores(), m_split_log_dir(""), m_split_point("") {
      return;
    }

    void get_table_name(std::string &tableName) {
      boost::mutex::scoped_lock lock(m_mutex);
      tableName = m_table_name;
    }
    void set_table_name(std::string &tableName) {
      boost::mutex::scoped_lock lock(m_mutex);
      m_table_name = tableName;
    }

    void get_start_row(std::string &startRow) {
      boost::mutex::scoped_lock lock(m_mutex);
      startRow = m_start_row;
    }
    void set_start_row(std::string &startRow) {
      boost::mutex::scoped_lock lock(m_mutex);
      m_start_row = startRow;
    }

    void get_end_row(std::string &endRow) {
      boost::mutex::scoped_lock lock(m_mutex);
      endRow = m_end_row;
    }
    void set_end_row(std::string &endRow) {
      boost::mutex::scoped_lock lock(m_mutex);
      m_end_row = endRow;
    }

    void get_location(std::string &location) {
      boost::mutex::scoped_lock lock(m_mutex);
      location = m_location;
    }
    void set_location(std::string &location) {
      boost::mutex::scoped_lock lock(m_mutex);
      m_location = location;
    }

    void get_log_dir(std::string &logDir) {
      boost::mutex::scoped_lock lock(m_mutex);
      logDir = m_log_dir;
    }
    void set_log_dir(std::string &logDir) {
      boost::mutex::scoped_lock lock(m_mutex);
      m_log_dir = logDir;
    }

    void add_cell_store(std::string &cellStore) {
      boost::mutex::scoped_lock lock(m_mutex);
      cellStores.insert(cellStore);
    }
    void remove_cell_store(std::string &cellStore) {
      boost::mutex::scoped_lock lock(m_mutex);
      cellStores.erase(cellStore);
    }
    void get_tables(std::vector<std::string> &tableVec) {
      boost::mutex::scoped_lock lock(m_mutex);
      for (std::set<std::string>::iterator iter = cellStores.begin(); iter != cellStores.end(); iter++)
	tableVec.push_back(*iter);
    }

    void get_split_log_dir(std::string &splitLogDir) {
      boost::mutex::scoped_lock lock(m_mutex);
      splitLogDir = m_split_log_dir;
    }
    void set_split_log_dir(std::string &splitLogDir) {
      boost::mutex::scoped_lock lock(m_mutex);
      m_split_log_dir = splitLogDir;
    }

    void get_split_point(std::string &splitPoint) {
      boost::mutex::scoped_lock lock(m_mutex);
      splitPoint = m_split_point;
    }
    void set_split_point(std::string &splitPoint) {
      boost::mutex::scoped_lock lock(m_mutex);
      m_split_point = splitPoint;
    }

  private:
    boost::mutex m_mutex;
    std::string m_table_name;
    std::string m_start_row;
    std::string m_end_row;
    std::string m_location;
    std::string m_log_dir;
    std::set<std::string> cellStores;
    std::string m_split_log_dir;
    std::string m_split_point;
  };

  typedef boost::intrusive_ptr<RangeInfo> RangeInfoPtr;

}



#endif // HYPERTABLE_TABLETINFO_H

