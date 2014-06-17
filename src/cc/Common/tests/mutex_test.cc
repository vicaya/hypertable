#include "Common/Compat.h"
#include "Common/Mutex.h"
#include "Common/atomic.h"
#include "Common/TestUtils.h"
#include "Common/Init.h"

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace boost;

namespace {

volatile int g_vcount = 0;
int g_count = 0;
Mutex g_mutex;
RecMutex g_recmutex;
atomic_t g_av;

void test_loop(int n) {
  HT_BENCH(format("%d: loop", g_vcount), ++g_vcount, n);
}

void test_atomic(int n) {
  HT_BENCH1(format("%d: atomic", g_count), for (int i = n; i--;)
    atomic_inc(&g_av); g_count = atomic_read(&g_av), n);
}

void test_mutex(int n) {
  HT_BENCH(format("%d: mutex", g_count),
    ScopedLock lock(g_mutex); ++g_count, n);
}

void test_recmutex(int n) {
  HT_BENCH(format("%d: recmutex", g_count),
    ScopedRecLock lock(g_recmutex); ++g_count, n);
}

} // local namespace

int main(int ac, char *av[]) {
  init_with_policy<DefaultTestPolicy>(ac, av);
  int n = get_i32("num-items");

  run_test(bind(test_loop, n));
  run_test(bind(test_atomic, n));
  run_test(bind(test_mutex, n));
  run_test(bind(test_recmutex, n));
}
