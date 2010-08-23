/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include "Common/Config.h"
#include "Common/Init.h"

#include <iostream>

#include "../AccessGroupGarbageTracker.h"
#include "../Global.h"

using namespace Hypertable;
using namespace Config;

namespace {

  const char *schema_str =
  "<Schema>\n"
  "  <AccessGroup name=\"default\">\n"
  "    <ColumnFamily>\n"
  "      <Name>tag</Name>\n"
  "    </ColumnFamily>\n"
  "  </AccessGroup>\n"
  "</Schema>";


  const char *schema2_str =
  "<Schema>\n"
  "  <AccessGroup name=\"default\">\n"
  "    <ColumnFamily>\n"
  "      <MaxVersions>1</MaxVersions>\n"   
  "      <Name>tag</Name>\n"
  "    </ColumnFamily>\n"
  "  </AccessGroup>\n"
  "</Schema>";

  const char *schema3_str =
  "<Schema>\n"
  "  <AccessGroup name=\"default\">\n"
  "    <ColumnFamily>\n"
  "      <ttl>3600</ttl>\n"
  "      <Name>tag</Name>\n"
  "    </ColumnFamily>\n"
  "  </AccessGroup>\n"
  "</Schema>";

}


int main(int argc, char **argv) {

  init_with_policy<DefaultPolicy>(argc, argv);

  Global::access_group_garbage_compaction_threshold 
    = properties->get_i32("Hypertable.RangeServer.AccessGroup.GarbageCompactionThreshold.Percentage");

  {
    AccessGroupGarbageTracker tracker;

    SchemaPtr schema = Schema::new_instance(schema_str, strlen(schema_str));
    if (!schema->is_valid()) {
      HT_ERRORF("Schema Parse Error: %s", schema->get_error_string());
      exit(1);
    }
    Schema::AccessGroup *ag = schema->get_access_group("default");

    tracker.set_schema(schema, ag);
    HT_ASSERT(!tracker.check_needed(0));

    tracker.add_delete_count(1);
    HT_ASSERT(!tracker.check_needed(0));

    tracker.clear();

    int64_t split_size = properties->get_i64("Hypertable.RangeServer.Range.SplitSize");

    int64_t amount = split_size / 5;
    tracker.accumulate_data(amount);
    HT_ASSERT(!tracker.check_needed(0));

    tracker.add_delete_count(1);
    HT_ASSERT(tracker.check_needed(0));

    tracker.clear();
    tracker.add_delete_count(1);
    tracker.accumulate_data(split_size/20);
    HT_ASSERT(!tracker.check_needed(0));
    tracker.accumulate_data(split_size/10);
    HT_ASSERT(tracker.check_needed(0));

    tracker.clear();

    schema = Schema::new_instance(schema2_str, strlen(schema2_str));
    if (!schema->is_valid()) {
      HT_ERRORF("Schema Parse Error: %s", schema->get_error_string());
      exit(1);
    }
    ag = schema->get_access_group("default");

    tracker.set_schema(schema, ag);
    HT_ASSERT(!tracker.check_needed(0));

    tracker.accumulate_data(amount);
    HT_ASSERT(tracker.check_needed(0));
    HT_ASSERT(tracker.current_target() == split_size/10);

    tracker.clear();
    tracker.accumulate_data(amount);
    tracker.set_garbage_stats(1000000LL, 100000LL);
    HT_ASSERT(tracker.current_target() == split_size/10);
    HT_ASSERT(tracker.need_collection());
    
    tracker.clear();
    tracker.accumulate_data(amount);
    tracker.set_garbage_stats(1000000LL, 950000LL);
    HT_ASSERT(tracker.current_target() == 53687090);
    HT_ASSERT(!tracker.need_collection());

    tracker.clear();
    tracker.accumulate_data(amount);
    tracker.set_garbage_stats(1000000LL, 600000LL);
    HT_ASSERT(tracker.current_target() == split_size/10);
    HT_ASSERT(tracker.need_collection());

    tracker.clear();
    tracker.accumulate_data(amount);
    tracker.set_garbage_stats(1000000LL, 700000LL);
    HT_ASSERT(tracker.current_target() == 35791394);
    HT_ASSERT(tracker.need_collection());

    tracker.clear();
    tracker.accumulate_data(amount);
    tracker.set_garbage_stats(1000000LL, 500000LL);
    HT_ASSERT(tracker.current_target() == split_size/10);
    HT_ASSERT(tracker.need_collection());

    tracker.clear();
    tracker.accumulate_data(amount);
    tracker.accumulate_data(amount);
    tracker.set_garbage_stats(1000000LL, 500000LL);
    HT_ASSERT(tracker.current_target() == 42949672);
    HT_ASSERT(tracker.need_collection());

    tracker.clear();
    tracker.accumulate_data(amount);
    tracker.set_garbage_stats(1000000LL, 900000LL);
    HT_ASSERT(tracker.current_target() == 85899344);
    HT_ASSERT(!tracker.need_collection());

    tracker.clear();

    schema = Schema::new_instance(schema3_str, strlen(schema3_str));
    if (!schema->is_valid()) {
      HT_ERRORF("Schema Parse Error: %s", schema->get_error_string());
      exit(1);
    }
    ag = schema->get_access_group("default");

    tracker.set_schema(schema, ag);
    HT_ASSERT(!tracker.check_needed(0));

    time_t now = 1;

    tracker.clear(now);
    tracker.accumulate_expirable(amount/4);
    HT_ASSERT(!tracker.check_needed(0, 2));

    HT_ASSERT(tracker.check_needed(amount/2, 361));

    tracker.accumulate_expirable(amount/2);
    HT_ASSERT(!tracker.check_needed(0, 3));

    HT_ASSERT(tracker.check_needed(0, 361));

    tracker.clear(1);
    HT_ASSERT(!tracker.check_needed(0, 361));

  }

  return 0;
}

