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
void ScanContext::initialize(uint64_t ts, ScanSpec *ss, RangeSpec *range_, SchemaPtr &sp) {
  Schema::ColumnFamily *cf;
  uint32_t max_versions = 0;

  // set time interval
  if (ss) {
    interval.first = ss->interval.first;
    interval.second = (ss->interval.second != 0) ? std::min(ts, ss->interval.second) : ts;
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

    // start row
    start_row = spec->start_row;
    if (!spec->start_row_inclusive)
      start_row.append(1,1);  // bump to next row

    // end row
    if (spec->end_row[0] == 0)
      end_row = Key::END_ROW_MARKER;
    else {
      end_row = spec->end_row;
      if (spec->end_row_inclusive) {
        uint8_t last_char = spec->end_row[strlen(spec->end_row)-1];
        if (last_char == 0xff)
          end_row.append(1,1);    // bump to next row
        else
          end_row[strlen(spec->end_row)-1] = (last_char+1);
      }
    }
  }
  else {
    start_row = "";
    end_row = Key::END_ROW_MARKER;
  }

}
