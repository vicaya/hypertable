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

#include "Common/Logger.h"

#include "TableInfo.h"

using namespace Hypertable;


/**
 * 
 */
bool TableInfo::get_range(RangeT *range, RangePtr &rangePtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  string startRow = range->startRow;
  string endRow = range->endRow;

  RangeMapT::iterator iter = m_range_map.find(endRow);

  if (iter == m_range_map.end())
    return false;

  rangePtr = (*iter).second;

  if (rangePtr->start_row() != startRow)
    return false;

  return true;
}



/**
 * 
 */
void TableInfo::add_range(RangeInfoPtr &rangeInfoPtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  std::string rangeEndRow;

  rangeInfoPtr->get_end_row(rangeEndRow);

  RangeMapT::iterator iter = m_range_map.find(rangeEndRow);
  assert(iter == m_range_map.end());

  RangePtr rangePtr( new Range(m_schema, rangeInfoPtr) );

  m_range_map[rangeEndRow] = rangePtr;
}


/**
 * 
 */
bool TableInfo::find_containing_range(std::string row, RangePtr &rangePtr) {
  boost::mutex::scoped_lock lock(m_mutex);

  RangeMapT::iterator iter = m_range_map.lower_bound(row);

  if (iter == m_range_map.end())
    return false;

  if (row <= (*iter).second->start_row())
    return false;

  rangePtr = (*iter).second;

  return true;
}
