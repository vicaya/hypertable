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

#include <cassert>

#include "ScanContext.h"

using namespace hypertable;

const uint64_t ScanContext::END_OF_TIME = (uint64_t)-1;


/**
 *
 */
void ScanContext::initialize(uint64_t ts, ScanSpecificationT *ss, SchemaPtr &sp) {
  Schema::ColumnFamily *columnFamily;
  uint32_t cellLimit = 0;

  timestamp = ts;
  spec = ss;

  assert(sp);

  if (spec == 0)
    memset(familyMask, true, 256*sizeof(bool));
  else {
    memset(familyMask, false, 256*sizeof(bool));
    cellLimit = spec->cellLimit;
  }

  memset(familyInfo, 0, 256*sizeof(CellFilterInfoT));

  schemaPtr = sp;
  if (spec && spec->columns.size() > 0) {
    for (std::vector<const char *>::const_iterator iter = spec->columns.begin(); iter != spec->columns.end(); iter++) {
      columnFamily = schemaPtr->GetColumnFamily(*iter);
      assert(columnFamily);

      familyMask[columnFamily->id] = true;
      if (columnFamily->expireTime == 0)
	familyInfo[columnFamily->id].cutoffTime == 0;
      else
	familyInfo[columnFamily->id].cutoffTime == timestamp - (columnFamily->expireTime * 1000000);
      if (cellLimit == 0)
	familyInfo[columnFamily->id].cellLimit = columnFamily->cellLimit;
      else {
	if (columnFamily->cellLimit == 0)
	  familyInfo[columnFamily->id].cellLimit = cellLimit;
	else
	  familyInfo[columnFamily->id].cellLimit = (cellLimit < columnFamily->cellLimit) ? cellLimit : columnFamily->cellLimit;
      }
    }
  }
  else {
    list<Schema::AccessGroup *> *agList = schemaPtr->GetAccessGroupList();

    familyMask[0] = true;  // ROW_DELETE records have 0 column family, so this allows them to pass through
    for (list<Schema::AccessGroup *>::iterator agIter = agList->begin(); agIter != agList->end(); agIter++) {
      for (list<Schema::ColumnFamily *>::iterator cfIter = (*agIter)->columns.begin(); cfIter != (*agIter)->columns.end(); cfIter++) {
	if ((*cfIter)->id == 0) {
	  error = Error::RANGESERVER_SCHEMA_INVALID_CFID;
	  return;
	}
	familyMask[(*cfIter)->id] = true;
	if ((*cfIter)->expireTime == 0)
	  familyInfo[(*cfIter)->id].cutoffTime == 0;
	else
	  familyInfo[(*cfIter)->id].cutoffTime == timestamp - ((*cfIter)->expireTime * 1000000);

	if (cellLimit == 0)
	  familyInfo[(*cfIter)->id].cellLimit = (*cfIter)->cellLimit;
	else {
	  if ((*cfIter)->cellLimit == 0)
	    familyInfo[(*cfIter)->id].cellLimit = cellLimit;
	  else
	    familyInfo[(*cfIter)->id].cellLimit = (cellLimit < (*cfIter)->cellLimit) ? cellLimit : (*cfIter)->cellLimit;
	}
      }
    }
  }

  /**
   * Create Start Key and End Key
   */
  if (spec) {
    if (*spec->startRow != 0)
      startKeyPtr = CreateKey(FLAG_INSERT, spec->startRow, 0, 0, 0LL);
    if (*spec->endRow != 0)
      endKeyPtr = CreateKey(FLAG_INSERT, spec->endRow, 0xFF, 0, 0LL);
  }

}
