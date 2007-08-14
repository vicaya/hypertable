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

#include "ScanContext.h"

using namespace hypertable;

const uint64_t ScanContext::END_OF_TIME = (uint64_t)-1;


/**
 *
 */
void ScanContext::initialize(uint64_t ts, ScanSpecificationT *ss, SchemaPtr &sp) {
  Schema::ColumnFamily *columnFamily;

  timestamp = ts;
  spec = ss;

  if (spec == 0) {
    memset(familyMask, true, 256*sizeof(bool));
    memset(familyInfo, 0, 256*sizeof(CellFilterInfoT));
    remainingCells = (uint64_t)-1;
    return;
  }
  remainingCells = (spec->cellCount != 0) ? spec->cellCount : (uint64_t)-1;
  if (sp.get() != 0) {
    schemaPtr = sp;
    memset(familyMask, false, 256*sizeof(bool));
    if (spec->columns && spec->columns->len > 0) {
      for (uint32_t i=0; i<spec->columns->len; i++) {
	if ((columnFamily = schemaPtr->GetColumnFamily(spec->columns->data[i])) == 0) {
	  error = Error::RANGESERVER_SCHEMA_INVALID_CFID;
	  return;
	}
	familyMask[spec->columns->data[i]] = true;
	if (columnFamily->expireTime == 0)
	  familyInfo[spec->columns->data[i]].cutoffTime == 0;
	else
	  familyInfo[spec->columns->data[i]].cutoffTime == timestamp - (columnFamily->expireTime * 1000000);
	familyInfo[spec->columns->data[i]].keepCopies = columnFamily->keepCopies;
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
	  familyInfo[(*cfIter)->id].keepCopies = (*cfIter)->keepCopies;
	}
      }
    }
  }
  else {
    memset(familyMask, true, 256*sizeof(bool));
    memset(familyInfo, 0, 256*sizeof(CellFilterInfoT));
  }

  /**
   * Create End Key
   */
  if (spec->endRow->len != 0)
    endKeyPtr = CreateKey(FLAG_INSERT, (const char *)spec->endRow->data, 0xFF, 0, 0LL);

}
