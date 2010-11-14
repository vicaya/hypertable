/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#include<re2/re2.h>

#include <cassert>
#include <utility>
#include <set>

#include "Common/ByteString.h"
#include "Common/Error.h"
#include "Common/ReferenceCount.h"
#include "Common/StringExt.h"

#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/ScanSpec.h"
#include "Hypertable/Lib/Types.h"

namespace Hypertable {

  using namespace std;

  class CellFilterInfo {
  public:
    CellFilterInfo(): cutoff_time(0), max_versions(0), counter(false),
        filter_by_qualifier(false) {}

    CellFilterInfo(const CellFilterInfo& other) {
      cutoff_time = other.cutoff_time;
      max_versions = other.max_versions;
      counter = other.counter;
      for (size_t ii=0; ii<other.regexp_qualifiers.size(); ++ii) {
        regexp_qualifiers[ii] = new RE2(other.regexp_qualifiers[ii]->pattern());
      }
      exact_qualifiers = other.exact_qualifiers;
      for (size_t ii=0; ii<exact_qualifiers.size(); ++ii) {
        exact_qualifiers_set.insert(exact_qualifiers[ii].c_str());
      }
      filter_by_qualifier = other.filter_by_qualifier;
    }

    ~CellFilterInfo() {
      for (size_t ii=0; ii<regexp_qualifiers.size(); ++ii)
        delete regexp_qualifiers[ii];
    }

    bool qualifier_matches(const char *qualifier) {
      if (!filter_by_qualifier)
        return true;
      // check exact match first
      if (exact_qualifiers_set.find(qualifier) != exact_qualifiers_set.end())
        return true;
      // check for regexp match
      for (size_t ii=0; ii<regexp_qualifiers.size(); ++ii)
        if (RE2::PartialMatch(qualifier, *regexp_qualifiers[ii]))
          return true;
      return false;
    }

    void add_qualifier(const char *qualifier, bool is_regexp) {
      if (is_regexp) {
        RE2 *regexp = new RE2(qualifier);
        if (!regexp->ok())
          HT_THROW(Error::BAD_SCAN_SPEC, (String)"Can't convert qualifier " + qualifier +
                   " to regexp -" + regexp->error_arg());
        regexp_qualifiers.push_back(regexp);
      }
      else {
        exact_qualifiers.push_back(qualifier);
        exact_qualifiers_set.insert(exact_qualifiers.back().c_str());
      }
      filter_by_qualifier = true;
    }

    bool has_qualifier_filter() const { return filter_by_qualifier; }

    int64_t  cutoff_time;
    uint32_t max_versions;
    bool counter;
  private:
    // disable assignment -- if needed then implement with deep copy of
    // qualifier_regexp
    CellFilterInfo& operator = (const CellFilterInfo&);
    vector<RE2 *> regexp_qualifiers;
    vector<String> exact_qualifiers;
    typedef set<const char *, LtCstr> QualifierSet;
    QualifierSet exact_qualifiers_set;
    bool filter_by_qualifier;
  };

  /**
   * Scan context information
   */
  class ScanContext : public ReferenceCount {
  public:
    SchemaPtr schema;
    const ScanSpec *spec;
    const RangeSpec *range;
    DynamicBuffer dbuf;
    SerializedKey start_serkey, end_serkey;
    Key start_key, end_key;
    String start_row, end_row;
    String start_qualifier, end_qualifier;
    bool start_inclusive, end_inclusive;
    bool single_row;
    bool has_cell_interval;
    bool has_start_cf_qualifier;
    bool restricted_range;
    int64_t revision;
    pair<int64_t, int64_t> time_interval;
    bool family_mask[256];
    vector<CellFilterInfo> family_info;
    RE2 *row_regexp;
    RE2 *value_regexp;

    /**
     * Constructor.
     *
     * @param rev scan revision
     * @param ss scan specification
     * @param range range specifier
     * @param schema smart pointer to schema object
     */
    ScanContext(int64_t rev, const ScanSpec *ss, const RangeSpec *range,
                SchemaPtr &schema) : family_info(256), row_regexp(0), value_regexp(0) {
      initialize(rev, ss, range, schema);
    }

    /**
     * Constructor.
     *
     * @param rev scan revision
     * @param schema smart pointer to schema object
     */
    ScanContext(int64_t rev, SchemaPtr &schema) : family_info(256), row_regexp(0),
        value_regexp(0) {
      initialize(rev, 0, 0, schema);
    }

    /**
     * Constructor.  Calls initialize() with an empty schema pointer.
     *
     * @param rev scan revision
     */
    ScanContext(int64_t rev=TIMESTAMP_MAX) : family_info(256), row_regexp(0), value_regexp(0) {
      SchemaPtr schema;
      initialize(rev, 0, 0, schema);
    }

    /**
     * Constructor.
     *
     * @param schema smart pointer to schema object
     */
    ScanContext(SchemaPtr &schema) : family_info(256), row_regexp(0), value_regexp(0) {
      initialize(TIMESTAMP_MAX, 0, 0, schema);
    }

    ~ScanContext() {
      if (row_regexp != 0) {
        delete row_regexp;
      }
      if (value_regexp != 0) {
        delete value_regexp;
      }
    }

  private:

    /**
     * Initializes the scan context.  Sets up the family_mask filter that
     * allows for quick lookups to see if a family is included in the scan.
     * Also sets up family_info entries for the column families that are
     * included in the scan which contains cell garbage collection info for
     * each family (e.g. cutoff timestamp and number of copies to keep).  Also
     * sets up end_row to be the last possible key in spec->end_row.
     *
     * @param rev scan revision
     * @param ss scan specification
     * @param range range specifier
     * @param sp shared pointer to schema object
     */
    void initialize(int64_t rev, const ScanSpec *ss, const RangeSpec *range,
                    SchemaPtr &sp);
    /**
     * Disable copy ctor and assignment op
     */
    ScanContext(const ScanContext&);
    ScanContext& operator = (const ScanContext&);
  };

  typedef intrusive_ptr<ScanContext> ScanContextPtr;

} // namespace Hypertable

#endif // HYPERTABLE_SCANCONTEXT_H
