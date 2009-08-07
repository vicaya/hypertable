#include "Common/Compat.h"
#include "Common/CharArena.h"
#include "Common/Logger.h"
#include "Common/Stopwatch.h"
#include "Common/CharArena.h"
#include "Common/Init.h"
#include "Common/SystemInfo.h"

#include <stdlib.h>
#include <vector>

#define MEASURE(_label_, _code_, _n_) do { \
  Stopwatch w; \
  _code_; \
  w.stop(); \
  std::cout << _label_ <<": "<< (_n_) / w.elapsed() <<"/s" << std::endl; \
} while (0)

#define MEASURE_ALLOC(_label_, _code_, _n_) do { \
  srandom(8); \
  long size; \
  MEASURE(_label_, for (int i = _n_; i--; ) { \
    size = (long)((double)random()/RAND_MAX * 120 + 4);  _code_; }, _n_); \
} while (0)

using namespace Hypertable;

namespace {

typedef std::vector<char *> Cstrs;
typedef std::vector<String> Strings;

void random_loop(int n) {
  MEASURE_ALLOC("loop", (void)size, n);
}

void random_malloc_test(Cstrs &v, int n) {
  v.clear();

  MEASURE_ALLOC("malloc", char *c = (char *) malloc(size + 1);
    for (int j = 0; j < size; ++j) c[j] = size;
    c[size] = 0;
    v.push_back(c), n);
}

void
random_pagearena_test(const char *label, CharArena &arena, Cstrs &v, int n) {
  v.clear();

  MEASURE_ALLOC(label, char *c = arena.alloc(size + 1);
    for (int j = 0; j < size; ++j) c[j] = size;
    c[size] = 0;
    v.push_back(c), n);

  std::cout << label <<": "<< arena << std::endl;
}

void random_strings_test(Strings &v, int n) {
  v.clear();

  MEASURE_ALLOC("strings", String str(size, size); v.push_back(str), n);
}

void assert_same(const Cstrs &v1, const Cstrs &v2) {
  HT_ASSERT(v1.size() == v2.size());

  for (size_t i = 0; i < v1.size(); ++i)
    HT_ASSERT(strcmp(v1[i], v2[i]) == 0);
}

void assert_same(const Cstrs &v1, const Strings &v2) {
  HT_ASSERT(v1.size() == v2.size());

  for (size_t i = 0; i < v1.size(); ++i) {
    HT_ASSERT(v2[i] == v1[i]);
  }
}

void random_test(int n) {
  // calibration
  random_loop(n);

  // allocation
  Cstrs v, v2;
  random_malloc_test(v, n);

  CharArena arena; // default
  random_pagearena_test("arena default", arena, v2, n);
  assert_same(v, v2);

  CharArena arena2(256); // stress
  random_pagearena_test("arena 256", arena2, v2, n);
  assert_same(v, v2);

  CharArena arena3(65536); // larger pages
  random_pagearena_test("arena 65536", arena3, v2, n);
  assert_same(v, v2);

  Strings v3;
  random_strings_test(v3, n);
  assert_same(v, v3);

  // deallocation
  MEASURE("free", for (int i = 0; i < n; ++i) free(v[i]), n);
  MEASURE("arena default free", arena.free(), n);
  MEASURE("arena 256 free", arena2.free(), n);
  MEASURE("arena 65536 free", arena3.free(), n);
  MEASURE("strings clear", v3.clear(), n);
}

void test_malloc_frag(int n) {
  Cstrs v;
  random_malloc_test(v, n);
  std::cout << System::proc_stat() << std::endl;
  MEASURE("free", foreach(char *c, v) free(c), n);
  std::cout << System::proc_stat() << std::endl;
  random_malloc_test(v, n);
  std::cout << System::proc_stat() << std::endl;
  MEASURE("free", foreach(char *c, v) free(c), n);
}

void test_pagearena_frag(int n) {
  Cstrs v;
  CharArena arena;
  random_pagearena_test("arena default", arena, v, n);
  std::cout << System::proc_stat() << std::endl;
  MEASURE("arena default free", arena.free(), n);
  std::cout << System::proc_stat() << std::endl;
  random_pagearena_test("arena default", arena, v, n);
  std::cout << System::proc_stat() << std::endl;
  MEASURE("arena default free", arena.free(), n);
}

void test_strings_frag(int n) {
  Strings v;
  random_strings_test(v, n);
  std::cout << System::proc_stat() << std::endl;
  MEASURE("strings clear", v.clear(), n);
  std::cout << System::proc_stat() << std::endl;
  random_strings_test(v, n);
  std::cout << System::proc_stat() << std::endl;
  MEASURE("strings clear", v.clear(), n);
}

} // local namespace

int main(int ac, char *av[]) {
  Config::init(ac, av);

  if (ac > 2) {
    int n = atoi(av[1]);

    if (!strcmp(av[2], "malloc"))
      test_malloc_frag(n);
    else if (!strcmp(av[2], "arena"))
      test_pagearena_frag(n);
    else if (!strcmp(av[2], "string"))
      test_strings_frag(n);

    std::cout << System::proc_stat() << std::endl;
  }
  else
    random_test(ac > 1 ? atoi(av[1]) : 100000);

  return 0;
}
