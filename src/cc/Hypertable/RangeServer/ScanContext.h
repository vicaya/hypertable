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

#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Types.h"

namespace Hypertable {

  typedef struct {
    uint64_t cutoffTime;
    uint32_t cellLimit;
  } CellFilterInfoT;

  /**
   * Scan context information
   */
  class ScanContext {
  public:

    static const uint64_t END_OF_TIME;

    SchemaPtr schemaPtr;
    ScanSpecificationT *spec;
    ByteString32Ptr endKeyPtr;
    ByteString32Ptr startKeyPtr;
    uint64_t timestamp;
    int error;
    bool familyMask[256];
    CellFilterInfoT familyInfo[256];

    /**
     * Constructor.
     *
     * @param ts scan timestamp (point in time when scan began)
     * @param ss scan specification (can be NULL)
     * @param sp shared pointer to schema object
     */
    ScanContext(uint64_t ts, ScanSpecificationT *ss, SchemaPtr &sp) : endKeyPtr(), error(Error::OK) {
      initialize(ts, ss, sp);
    }

    /**
     * Constructor.  Calls initialize() with an empty schema pointer.
     *
     * @param ts scan timestamp (point in time when scan began)
     * @param ss scan specification (can be NULL)
     */
    ScanContext(uint64_t ts, ScanSpecificationT *ss) : endKeyPtr(), error(Error::OK) {
      SchemaPtr schemaPtr;
      initialize(ts, ss, schemaPtr);
    }

  private:

    /**
     * Initializes the scan context.  Sets up the familyMask filter that
     * allows for quick lookups to see if a family is included in the scan.  Also sets
     * up familyInfo entries for the column families that are included in the scan
     * which contains cell garbage collection info for each family (e.g. cutoff
     * timestamp and number of copies to keep).  Also sets up endKeyPtr to be the
     * last possible key in spec->endRow.
     *
     * @param ts scan timestamp (point in time when scan began)
     * @param ss scan specification
     * @param sp shared pointer to schema object
     */
    void initialize(uint64_t ts, ScanSpecificationT *ss, SchemaPtr &sp);

  };

  typedef boost::shared_ptr<ScanContext> ScanContextPtr;


}

#endif // HYPERTABLE_SCANCONTEXT_H
