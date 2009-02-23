/** -*- C++ -*-
 * Copyright (C) 2009  Luke Lu (Zvents, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include "Common/Compat.h"
#include "Common/Config.h"
#include "Common/BloomFilter.h"
#include "Common/Logger.h"
#include "Common/Stopwatch.h"
#include "Common/Lookup3.h"
#include "Common/SuperFastHash.h"
#include "Common/MurmurHash.h"
#include "Common/md5.h"

using namespace std;
using namespace Hypertable;
using namespace Config;

namespace {

struct MyPolicy : Config::Policy {
  static void init_options() {
    cmdline_desc("Usage: %s [Options] [<num_items>]\nOptions").add_options()
      ("MurmurHash2", "Test with MurmurHash2 by Austin Appleby")
      ("Lookup3", "Test with Lookup3 by Bob Jenkins")
      ("SuperFastHash", "Test with SuperFastHash by Paul Hsieh")
      ("length", i16()->default_value(32), "length of test strings")
      ("false-positive,p", f64()->default_value(0.01),
          "false positive probability for Bloomfilter")
      ;
    cmdline_hidden_desc().add_options()
      ("items,n", i32()->default_value(200*K), "number of items")
      ;
    cmdline_positional_desc().add("items", -1);
  }
};

typedef Meta::list<MyPolicy, DefaultPolicy> Policies;

struct Item {
  String data;

  Item(int i, size_t len) {
    char buf[33], fmt[33];
    md5_hex(&i, sizeof(i), buf);
    sprintf(fmt, "%%-%ds", (int)len);
    data = format(fmt, buf);
  }
};

typedef std::vector<Item> Items;

#define MEASURE(_label_, _code_, _n_) do { \
  Stopwatch w; _code_; w.stop(); \
  cout << _label_ <<": "<< (_n_) / w.elapsed() <<"/s" << endl; \
} while (0)

#define TEST_IF(_hash_) do { \
  if (!has_choice || has(#_hash_)) test<_hash_>(#_hash_); \
} while (0)

struct BloomFilterTest {
  bool has_choice;
  double fp_prob;
  Items items;

  BloomFilterTest(int nitems, size_t len) {
    has_choice = has("Lookup3") || has("SuperFastHash") || has("MurmurHash2");
    fp_prob = get_f64("false-positive");
    double total = 0.;
    nitems *= 2;

    MEASURE("generating items",
      for (int i = 0; i < nitems; ++i) {
        items.push_back(Item(i, len));
        total += items.back().data.length();
      }, nitems);

    cout <<"  average length="<< total / items.size() << endl;
  }

  template <class HashT>
  void test(const String &label) {
    size_t nitems = items.size() / 2;
    BasicBloomFilter<HashT> filter(nitems, fp_prob);

    cout << label << endl;

    MEASURE("  insert", for (size_t i = 0; i < nitems; ++i)
      filter.insert(items[i].data), nitems);

    MEASURE("  true positives", for (size_t i = 0; i < nitems; ++i)
      HT_ASSERT(filter.may_contain(items[i].data)), nitems);

    double false_positives = 0.;
    size_t nfalses = items.size() - nitems;

    MEASURE("  false positives",
      for (size_t i = nitems, n = items.size(); i < n; ++i)
        if (filter.may_contain(items[i].data))
          ++false_positives, nfalses);

    cout << "  false positive rate: expected "<< fp_prob <<", got "
         << false_positives / nfalses << endl;
  }

  void run() {
    TEST_IF(Lookup3);
    TEST_IF(SuperFastHash);
    TEST_IF(MurmurHash2);
  }
};

} // local namespace

int main(int argc, char *argv[]) {
  try {
    init_with_policies<Policies>(argc, argv);

    BloomFilterTest test(get_i32("items"), get_i16("length"));

    test.run();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
}
