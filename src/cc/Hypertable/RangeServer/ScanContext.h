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
#ifndef HYPERTABLE_SCANCONTEXT_H
#define HYPERTABLE_SCANCONTEXT_H

#include <cassert>
#include <utility>

#include <boost/shared_ptr.hpp>

#include "Common/ByteString.h"
#include "Common/Error.h"

#include "Hypertable/Schema.h"
#include "Request.h"

namespace hypertable {

  typedef struct {
    uint64_t cutoffTime;
    uint32_t keepCopies;
  } CellFilterInfoT;

  /**
   * Scan context information
   */
  class ScanContext {
  public:

    /**
     * Constructor
     */
    ScanContext() : returnDeletes(false), gcbuf(0) { return; }

    /**
     * Destructor, deletes gcbuf 
     */
    ~ScanContext() { delete [] gcbuf; }

    void Initialize(uint64_t ts) {
      timestamp = ts;
      memset(familyMask, true, 256*sizeof(bool));
      memset(familyInfo, 0, 256*sizeof(CellFilterInfoT));
    }

    /**
     * Initializes the scan context.  Sets up the familyMask filter that allows for
     * quick lookups to see if a family is included in the scan.  Also sets up
     * familyInfo entries for the column families that are included in the scan
     * which contains cell garbage collection info for each family (e.g. cutoff
     * timestamp and number of copies to keep).
     *
     * @param sp shared pointer to schema object
     * @param ts scan timestamp (point in time when scan began)
     * @param ss scan specification
     * @return Error::OK on success, or other error code on failure
     */
    int Initialize(SchemaPtr &sp, uint64_t ts, ScanSpecificationT *ss) {
      Schema::ColumnFamily *columnFamily;
      schemaPtr = sp;
      spec = ss;
      timestamp = ts;
      assert(spec);
      remainingCells = (spec->cellCount != 0) ? spec->cellCount : (uint64_t)-1;
      memset(familyMask, false, 256*sizeof(bool));
      if (spec->columns && spec->columns->len > 0) {
	for (uint32_t i=0; i<spec->columns->len; i++) {
	  if ((columnFamily = schemaPtr->GetColumnFamily(spec->columns->data[i])) == 0)
	    return Error::RANGESERVER_SCHEMA_INVALID_CFID;
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

	for (list<Schema::AccessGroup *>::iterator agIter = agList->begin(); agIter != agList->end(); agIter++) {
	  for (list<Schema::ColumnFamily *>::iterator cfIter = (*agIter)->columns.begin(); cfIter != (*agIter)->columns.end(); cfIter++) {
	    if ((*cfIter)->id == 0)
	      return Error::RANGESERVER_SCHEMA_INVALID_CFID;
	    familyMask[(*cfIter)->id] = true;
	    if ((*cfIter)->expireTime == 0)
	      familyInfo[(*cfIter)->id].cutoffTime == 0;
	    else
	      familyInfo[(*cfIter)->id].cutoffTime == timestamp - ((*cfIter)->expireTime * 1000000);
	    familyInfo[(*cfIter)->id].keepCopies = (*cfIter)->keepCopies;
	  }
	}
      }
      return Error::OK;
    }

    /**
     * Makes a deep copy of the ScanSpecification.  Entire specification is written
     * to a single array 'gcbuf' that gets deleted in the destructor.
     */
    void CopyScanSpecification() {
      if (spec) {
	int32_t len = sizeof(ScanSpecificationT) + Length(spec->columns) + Length(spec->startRow) + Length(spec->endRow);
	ScanSpecificationT *oldSpec = spec;
	uint8_t *ptr;
	gcbuf = new uint8_t [ len ];
	spec = (ScanSpecificationT *)gcbuf;
	memcpy(spec, oldSpec, sizeof(ScanSpecificationT));
	ptr = gcbuf + sizeof(ScanSpecificationT);

	memcpy(ptr, oldSpec->columns, Length(oldSpec->columns));
	spec->columns = (ByteString32T *)ptr;
	ptr += Length(oldSpec->columns);

	memcpy(ptr, oldSpec->startRow, Length(oldSpec->startRow));
	spec->startRow = (ByteString32T *)ptr;
	ptr += Length(oldSpec->startRow);

	memcpy(ptr, oldSpec->endRow, Length(oldSpec->endRow));
	spec->endRow = (ByteString32T *)ptr;
	ptr += Length(oldSpec->endRow);
      }
    }
    
    SchemaPtr schemaPtr;
    ScanSpecificationT *spec;
    uint8_t *gcbuf;
    bool returnDeletes;
    uint64_t timestamp;
    uint64_t remainingCells;
    bool familyMask[256];
    CellFilterInfoT familyInfo[256];
  };

  typedef boost::shared_ptr<ScanContext> ScanContextPtr;


}

#endif // HYPERTABLE_SCANCONTEXT_H
