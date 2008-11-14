#include "Common/Compat.h"
#include "Common/Thread.h"
#include "Common/Mutex.h"
#include "Common/Stopwatch.h"
#include <boost/thread.hpp>
#include "Common/atomic.h"

using namespace Hypertable;

#define MEASURE(_label_, _code_, _n_) do { \
  Stopwatch w; \
  _code_; \
  w.stop(); \
  std::cout << _count <<": "<<_label_ <<": "<< (_n_) / w.elapsed()  <<"/s" \
            << std::endl; \
} while (0)

namespace {

volatile int _count = 0;
boost::mutex _mutex;
boost::recursive_mutex _recmutex;

atomic_t _av;

void test_loop(int n) {
  MEASURE("loop", for (int i = n; i--;) ++_count, n);
}

void test_atomic(int n) {
  MEASURE("atomic", for (int i = n; i--;) atomic_inc(&_av);
    _count = atomic_read(&_av), n);
}

void test_mutex(int n) {
  MEASURE("mutex", for (int i = n; i--;) {
    boost::mutex::scoped_lock lock(_mutex);
    ++_count;
  }, n);
}

void test_recmutex(int n) {
  MEASURE("recmutex", for (int i = n; i--;) {
    boost::recursive_mutex::scoped_lock lock(_recmutex);
    ++_count;
  }, n);
}

} // local namespace

int main(int ac, char *av[]) {
  int n = ac > 1 ? atoi(av[1]) : 1000000;

  test_loop(n);
  test_atomic(n);
  test_mutex(n);
  test_recmutex(n);

  return 0;
}
