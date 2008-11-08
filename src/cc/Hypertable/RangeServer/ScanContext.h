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

#ifndef HYPERTABLE_SCANCONTEXT_H
#define HYPERTABLE_SCANCONTEXT_H

#include <cassert>
#include <utility>

#include "Common/ByteString.h"
#include "Common/Error.h"
#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/ScanSpec.h"
#include "Hypertable/Lib/Types.h"

namespace Hypertable {

  struct CellFilterInfo {
    int64_t  cutoff_time;
    uint32_t max_versions;
  };

  /**
   * Scan context information
   */
  class ScanContext : public ReferenceCount {
  public:

    SchemaPtr schema_ptr;
    ScanSpec *spec;
    RangeSpec *range;
    DynamicBuffer dbuf;
    SerializedKey start_key, end_key;
    String start_row, end_row;
    bool single_row;
    int64_t revision;
    std::pair<int64_t, int64_t> time_interval;
    bool family_mask[256];
    CellFilterInfo family_info[256];

    /**
     * Constructor.
     *
     * @param rev scan revision
     * @param ss scan specification
     * @param range_ range specifier
     * @param sp shared pointer to schema object
     */
    ScanContext(int64_t rev, ScanSpec *ss, RangeSpec *range_, SchemaPtr &sp) {
      initialize(rev, ss, range_, sp);
    }

    /**
     * Constructor.
     *
     * @param rev scan revision
     * @param sp shared pointer to schema object
     */
    ScanContext(int64_t rev, SchemaPtr &sp) {
      initialize(rev, 0, 0, sp);
    }

    /**
     * Constructor.  Calls initialize() with an empty schema pointer.
     *
     * @param rev scan revision
     */
    ScanContext(int64_t rev=TIMESTAMP_MAX) {
      SchemaPtr schema_ptr;
      initialize(rev, 0, 0, schema_ptr);
    }

    /**
     * Constructor.
     *
     * @param sp shared pointer to schema object
     */
    ScanContext(SchemaPtr &sp) {
      initialize(TIMESTAMP_MAX, 0, 0, sp);
    }


  private:

    /**
     * Initializes the scan context.  Sets up the family_mask filter that
     * allows for quick lookups to see if a family is included in the scan.  Also sets
     * up family_info entries for the column families that are included in the scan
     * which contains cell garbage collection info for each family (e.g. cutoff
     * timestamp and number of copies to keep).  Also sets up end_row to be the
     * last possible key in spec->end_row.
     *
     * @param rev scan revision
     * @param ss scan specification
     * @param range_ range specifier
     * @param sp shared pointer to schema object
     */
    void initialize(int64_t rev, ScanSpec *ss, RangeSpec *range_, SchemaPtr &sp);

  };

  typedef boost::intrusive_ptr<ScanContext> ScanContextPtr;


}

#endif // HYPERTABLE_SCANCONTEXT_H
