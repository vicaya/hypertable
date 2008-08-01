/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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
#include <algorithm>
#include <cassert>

#include "Common/Logger.h"

#include "Hypertable/Lib/Key.h"

#include "ScanContext.h"

using namespace std;
using namespace Hypertable;

/**
 *
 */
void ScanContext::initialize(int64_t ts, ScanSpec *ss, RangeSpec *range_, SchemaPtr &sp) {
  Schema::ColumnFamily *cf;
  uint32_t max_versions = 0;

  // set time interval
  if (ss) {
    interval.first = ss->time_interval.first;
    if (ts == 0)
      interval.second = ss->time_interval.second;
    else
      interval.second = (ss->time_interval.second != 0) ? std::min(ts, ss->time_interval.second) : ts;
  }
  else {
    interval.first = 0;
    interval.second = ts;
  }

  if (interval.second == 0)
    interval.second = END_OF_TIME;

  spec = ss;
  range = range_;

  if (spec == 0)
    memset(family_mask, true, 256*sizeof(bool));
  else {
    memset(family_mask, false, 256*sizeof(bool));
    max_versions = spec->max_versions;
  }

  memset(family_info, 0, 256*sizeof(CellFilterInfo));

  if (sp) {

    schema_ptr = sp;
    if (spec && spec->columns.size() > 0) {
      for (std::vector<const char *>::const_iterator iter = spec->columns.begin(); iter != spec->columns.end(); iter++) {
        cf = schema_ptr->get_column_family(*iter);

        if (cf == 0)
          throw Hypertable::Exception(Error::RANGESERVER_INVALID_COLUMNFAMILY, *iter);

        family_mask[cf->id] = true;
        if (cf->ttl == 0)
          family_info[cf->id].cutoff_time = 0;
        else
          family_info[cf->id].cutoff_time = ts - ((uint64_t)cf->ttl * 1000000000LL);
        if (max_versions == 0)
          family_info[cf->id].max_versions = cf->max_versions;
        else {
          if (cf->max_versions == 0)
            family_info[cf->id].max_versions = max_versions;
          else
            family_info[cf->id].max_versions = (max_versions < cf->max_versions) ? max_versions : cf->max_versions;
        }
      }
    }
    else {
      list<Schema::AccessGroup *> *aglist = schema_ptr->get_access_group_list();

      family_mask[0] = true;  // ROW_DELETE records have 0 column family, so this allows them to pass through
      for (list<Schema::AccessGroup *>::iterator ag_it = aglist->begin(); ag_it != aglist->end(); ag_it++) {
        for (list<Schema::ColumnFamily *>::iterator cf_it = (*ag_it)->columns.begin(); cf_it != (*ag_it)->columns.end(); cf_it++) {
          if ((*cf_it)->id == 0)
            throw Hypertable::Exception(Error::RANGESERVER_SCHEMA_INVALID_CFID, (std::string)"Bad ID for Column Family '" + (*cf_it)->name + "'");
          family_mask[(*cf_it)->id] = true;
          if ((*cf_it)->ttl == 0)
            family_info[(*cf_it)->id].cutoff_time = 0;
          else
            family_info[(*cf_it)->id].cutoff_time = ts - ((uint64_t)(*cf_it)->ttl * 1000000000LL);

          if (max_versions == 0)
            family_info[(*cf_it)->id].max_versions = (*cf_it)->max_versions;
          else {
            if ((*cf_it)->max_versions == 0)
              family_info[(*cf_it)->id].max_versions = max_versions;
            else
              family_info[(*cf_it)->id].max_versions = (max_versions < (*cf_it)->max_versions) ? max_versions : (*cf_it)->max_versions;
          }
        }
      }
    }
  }

  /**
   * Create Start Key and End Key
   */
  if (spec) {
    if (!spec->row_intervals.empty()) {

      // start row
      start_row = spec->row_intervals[0].start;
      if (!spec->row_intervals[0].start_inclusive)
	start_row.append(1,1);  // bump to next row

      // end row
      if (spec->row_intervals[0].end[0] == 0)
	end_row = Key::END_ROW_MARKER;
      else {
	end_row = spec->row_intervals[0].end;
	if (spec->row_intervals[0].end_inclusive) {
	  uint8_t last_char = spec->row_intervals[0].end[end_row.length()-1];
	  if (last_char == 0xff)
	    end_row.append(1,1);    // bump to next row
	  else
	    end_row[end_row.length()-1] = (last_char+1);
	}
      }
    }
    else if (!spec->cell_intervals.empty()) {
      std::string column_family_str;
      Schema::ColumnFamily *cf;

      if (*spec->cell_intervals[0].start_column) {
	const char *ptr = strchr(spec->cell_intervals[0].start_column, ':');
	if (ptr == 0)
	  HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC,
		   format("Bad cell spec (%s)", spec->cell_intervals[0].start_column));
	column_family_str = std::string(spec->cell_intervals[0].start_column, ptr-spec->cell_intervals[0].start_column);
	if ((cf = schema_ptr->get_column_family(column_family_str)) == 0)
	  HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC,
		   format("Bad column family (%s)", column_family_str.c_str()));
	ptr++;
	start_row = spec->cell_intervals[0].start_row + std::string(1, 0) + std::string(1, (char)cf->id) + ptr;
	if (!spec->cell_intervals[0].start_inclusive)
	  start_row.append(1,1);  // bump to next cell
      }
      else {
	// start row
	start_row = spec->cell_intervals[0].start_row;
      }

      if (*spec->cell_intervals[0].end_column) {
	const char *ptr = strchr(spec->cell_intervals[0].end_column, ':');
	if (ptr == 0)
	  HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC,
		   format("Bad cell spec (%s)", spec->cell_intervals[0].end_column));
	column_family_str = std::string(spec->cell_intervals[0].end_column, ptr-spec->cell_intervals[0].end_column);
	if ((cf = schema_ptr->get_column_family(column_family_str)) == 0)
	  HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC,
		   format("Bad column family (%s)", column_family_str.c_str()));
	ptr++;
	end_row = spec->cell_intervals[0].end_row + std::string(1, 0) + std::string(1, (char)cf->id) + ptr;
	if (spec->cell_intervals[0].end_inclusive)
	  end_row.append(1,1);  // bump to next cell
      }
      else {
	// end row
	end_row = spec->cell_intervals[0].end_row;
      }
    }
    else {
      start_row = "";
      end_row = Key::END_ROW_MARKER;
    }

    if (start_row.compare(end_row) > 0)
      HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC, "start_cell > end_cell");
  }
  else {
    start_row = "";
    end_row = Key::END_ROW_MARKER;
  }

}
