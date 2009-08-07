#include "Common/Compat.h"

#include <vector>

#include "Common/Init.h"
#include "Common/Stopwatch.h"
#include "Common/md5.h"
#include "Common/HashMap.h"
#include "Common/StlHash.h"
#include "Common/TclHash.h"
#include "Common/Lookup3.h"
#include "Common/SuperFastHash.h"
#include "Common/MurmurHash.h"
#include "Common/BlobHashSet.h"
#include "Common/CstrHashMap.h"
#include "Common/SystemInfo.h"

using namespace Hypertable;
using namespace Config;
using namespace std;

namespace {

struct MyPolicy : Config::Policy {
  static void init_options() {
    cmdline_desc("Usage: %s [Options] [<num_items>]\nOptions").add_options()
      ("SgiHash", "Test SGI STL hash")
      ("TclHash", "Test Tcl hash")
      ("TclHash2", "Test manually unrolled Tcl hash")
      ("Tr1Hash", "Test STL TR1 hash")
      ("MurmurHash2", "Test MurmurHash2 by Austin Appleby")
      ("Lookup3", "Test Lookup3 by Bob Jenkins")
      ("SuperFastHash", "Test SuperFastHash by Paul Hsieh")
      ("StringHashSet", "Test with hash_set<string>")
      ("CstrHashMap", "Test with CstrHashMap")
      ("BlobHashSet", "Test with BlobHashSet")
      ("repeats,r", i32()->default_value(1), "number of repeats for tests")
      ("length", i16()->default_value(7), "length of test strings")
      ;
    cmdline_hidden_desc().add_options()
      ("items,n", i32()->default_value(100*K), "number of items")
      ;
    cmdline_positional_desc().add("items", -1);
  }
};

typedef Cons<MyPolicy, DefaultPolicy> AppPolicy;

struct HashItem {
  HashItem(size_t i, size_t len) {
    char buf[33], fmt[33];
    md5_hex(&i, sizeof(i), buf);
    sprintf(fmt, "%%-%ds", (int)len);
    String tmp = format(fmt, buf);
    data = String(tmp.data(), len);
  }
  String data;
};

typedef std::vector<HashItem> Items;

#define MEASURE(_label_, _code_, _n_) do { \
  Stopwatch w; _code_; w.stop(); \
  cout << _label_ <<": "<< (_n_) / w.elapsed() <<"/s" << endl; \
} while (0)

#define TEST_HASHSET_IF(_hash_set_, _code_) do { \
  if (!has_set_choice || has(#_hash_set_)) \
    for (int r = 0; r < repeats; ++r) { _code_;  } \
} while (0)

#define TEST_IF(_hash_) do { \
  if (!has_hash_choice || has(#_hash_)) test<_hash_>(#_hash_, repeats); \
} while (0)

struct HashTest {
  HashTest(size_t nitems, size_t len) {
    has_hash_choice = has("SgiHash") || has("Tr1Hash") || has("TclHash")
                      || has("TclHash2") || has("Lookup3")
                      || has("SuperFastHash") || has("MurmurHash2");
    has_set_choice = has("StringHashSet") || has("BlobHashSet")
                     || has("CstrHashMap");

    MEASURE("generating items", for (size_t i = 0; i < nitems; ++i)
      items.push_back(HashItem(i, len)), nitems);

    uint32_t last_hash = 0;     // in case of optimistic optimizer

    MEASURE("  trivial loop",
      for (Items::const_iterator i = items.begin(), end = items.end();
           i != end; ++i) last_hash = i->data[0], items.size());

    MEASURE("  foreach loop",
      foreach(const HashItem &item, items)
        last_hash = item.data[0], items.size());

    cout <<"  length="<< len <<" last hash="<< last_hash << endl;
  }

  template <class HashT>
  void test(const String &label, int repeats) {
    hash_set<String, HashT> str_set(items.size());
    BlobHashSet<BlobHashTraits<HashT> > blob_set(items.size());
    CstrHashMap<size_t, CstrHashTraits<HashT> > cstr_map(items.size());
    uint32_t last_hash = 0;     // ditto

    for (int r = 0; r < repeats; ++r) {
      MEASURE(label, HashT hasher;
        foreach(const HashItem &hi, items) last_hash = hasher(hi.data),
        items.size());
    }

    cout <<"  last hash="<< last_hash << endl;

    // string hash set
    TEST_HASHSET_IF(StringHashSet,
      // common case: string needs to be constructed
      MEASURE("  string_hash_set insert",
        foreach(const HashItem &hi, items)
          str_set.insert(String(hi.data.data(), hi.data.size())),
        items.size());

      if (has("StringHashSet"))
        cout <<"  proc stats: "<< System::proc_stat() << endl;

      MEASURE("  string_hash_set lookup",
        foreach(const HashItem &hi, items)
          HT_ASSERT(str_set.find(String(hi.data.data(), hi.data.size()))
                    != str_set.end()),
        items.size());

      MEASURE("  string_hash_set clear", str_set.clear(), items.size()));

    // cstr_hash_map
    TEST_HASHSET_IF(CstrHashMap,
      MEASURE("  cstr_hash_map insert",
        foreach(const HashItem &hi, items)
          cstr_map.insert(hi.data.c_str(), hi.data.size()),
        items.size());

      MEASURE("  cstr_hash_map lookup",
        foreach(const HashItem &hi, items)
          HT_ASSERT(cstr_map.find(hi.data.c_str()) != cstr_map.end()),
        items.size());

      MEASURE("  cstr_hash_map clear", cstr_map.clear(), items.size()));

    // blob hash set
    TEST_HASHSET_IF(BlobHashSet,
      MEASURE("  blob_hash_set insert",
        foreach(const HashItem &hi, items) blob_set.insert(hi.data),
        items.size());

      if (has("BlobHashSet")) {
        cout <<"  allocator stats: "<< blob_set.key_alloc() << endl;
        cout <<"  proc stats: "<< System::proc_stat() << endl;
      }

      MEASURE("  blob_hash_set lookup",
        foreach(const HashItem &hi, items)
          HT_ASSERT(blob_set.find(hi.data) != blob_set.end()), items.size());

      MEASURE("  blob_hash_set clear", blob_set.clear(), items.size()));
  }

  void run() {
    int repeats = get_i32("repeats");

    TEST_IF(SgiHash);
    TEST_IF(Tr1Hash);
    TEST_IF(TclHash);
    TEST_IF(TclHash2);
    TEST_IF(Lookup3);
    TEST_IF(SuperFastHash);
    TEST_IF(MurmurHash2);
  }

  Items items;
  bool has_hash_choice;
  bool has_set_choice;
};

} // local namespace

int main(int ac, char *av[]) {
  try {
    init_with_policy<AppPolicy>(ac, av);

    HashTest hash_test(get_i32("items"), get_i16("length"));

    hash_test.run();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
}
