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

#include "Global.h"
#include "ScanContext.h"

using namespace std;
using namespace Hypertable;

/**
 *
 */
void ScanContext::initialize(int64_t rev, ScanSpec *ss, RangeSpec *range_, SchemaPtr &sp) {
  Schema::ColumnFamily *cf;
  uint32_t max_versions = 0;
  boost::xtime xtnow;
  int64_t now;

  boost::xtime_get(&xtnow, boost::TIME_UTC);
  now = ((int64_t)xtnow.sec * 1000000000LL) + (int64_t)xtnow.nsec;

  revision = rev;

  // set time interval
  if (ss) {
    time_interval.first = ss->time_interval.first;
    time_interval.second = ss->time_interval.second;
  }
  else {
    time_interval.first = TIMESTAMP_MIN;
    time_interval.second = TIMESTAMP_MAX;
  }

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
          family_info[cf->id].cutoff_time = TIMESTAMP_MIN;
        else
          family_info[cf->id].cutoff_time = now - ((uint64_t)cf->ttl * 1000000000LL);
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
            family_info[(*cf_it)->id].cutoff_time = now - ((uint64_t)(*cf_it)->ttl * 1000000000LL);

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

  String start_qualifier, end_qualifier;
  uint8_t start_family = 0;
  uint8_t end_family = 0;

  single_row = false;

  if (spec) {

    if (spec->row_limit == 1)
      single_row = true;

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

          if (!strcmp(spec->row_intervals[0].start, spec->row_intervals[0].end))
            single_row = true;

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

	start_family = cf->id;
	start_qualifier = ptr+1;

	start_row = spec->cell_intervals[0].start_row;
	if (!spec->cell_intervals[0].start_inclusive)
	  start_qualifier.append(1,1);  // bump to next cell
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

	end_family = cf->id;
	end_qualifier = ptr+1;

	end_row = spec->cell_intervals[0].end_row;
	if (spec->cell_intervals[0].end_inclusive)
	  end_qualifier.append(1,1);  // bump to next cell
      }
      else {
	// end row
	end_row = spec->cell_intervals[0].end_row;
      }

      if (!strcmp(spec->cell_intervals[0].start_row, spec->cell_intervals[0].end_row))
        single_row = true;

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

  assert(start_row <= end_row);

  dbuf.reserve(start_row.length() + start_qualifier.length() + end_row.length() + end_qualifier.length() + 64);

  create_key_and_append(dbuf, 0, start_row.c_str(), start_family, start_qualifier.c_str(), TIMESTAMP_MIN, TIMESTAMP_MIN);
  size_t offset = dbuf.ptr - dbuf.base;
  create_key_and_append(dbuf, 0, end_row.c_str(), end_family, end_qualifier.c_str(), TIMESTAMP_MIN, TIMESTAMP_MIN);

  start_key.ptr = dbuf.base;
  end_key.ptr = dbuf.base + offset;


}
