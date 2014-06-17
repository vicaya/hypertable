/** -*- C++ -*-
 * Copyright (C) 2009  Luke Lu (llu@hypertable.org)
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

#ifndef HYPERTABLE_TEST_UTILS_H
#define HYPERTABLE_TEST_UTILS_H

#include <cmath>
#include <limits>

#include <boost/bind.hpp>

#include "Config.h"
#include "SystemInfo.h"
#include "Stopwatch.h"
#include "Thread.h"

// some benchmark/test helpers for writing ad-hoc benchmark tests
#define HT_BENCH_OUT(_label_, _n_, _w_) do { \
  std::cout << ThisThread::get_id() <<": "<< (_label_) <<": " \
            << (_n_) / _w_.elapsed() <<"/s ("<< _w_.elapsed() / (_n_) * 1e6 \
            <<"ns per)"<< std::endl; \
} while(0)

#define HT_BENCH(_label_, _code_, _n_) do { \
  Stopwatch _w; \
  for (int i = 0, _n = (_n_); i < _n; ++i) { _code_; } \
  _w.stop(); \
  HT_BENCH_OUT(_label_, _n_, _w); \
} while (0)

#define HT_BENCH1(_label_, _code_, _n_) do { \
  Stopwatch _w; _code_; _w.stop(); \
  HT_BENCH_OUT(_label_, _n_, _w); \
} while (0)

namespace Hypertable {

inline void print_proc_stat() {
  std::cout << ThisThread::get_id() <<": "<< System::proc_stat() <<"\n\n";
}

// A test stat accumulator for min, max, mean and stdev
class TestStat {
 public:
  TestStat() : m_minv(std::numeric_limits<double>::max()),
               m_maxv(std::numeric_limits<double>::min()),
               m_i(0), m_a0(0.), m_a1(0.), m_q0(0.), m_q1(0.) {}

  void operator()(double x) {
    if (x < m_minv) m_minv = x;
    if (x > m_maxv) m_maxv = x;

    // The Welford method for numerical stability
    m_a1 = m_a0 + (x - m_a0) / ++m_i;
    m_q1 = m_q0 + (x - m_a0) * (x - m_a1);
    m_a0 = m_a1;
    m_q0 = m_q1;
  }

  // sync version
  void add(double x) {
    ScopedLock lock(m_mutex);
    (*this)(x);
  }

  double min() const { ScopedLock lock(m_mutex); return m_minv; }
  double max() const { ScopedLock lock(m_mutex); return m_maxv; }
  double mean() const { ScopedLock lock(m_mutex); return m_a1; }
  double stdev() const {
    ScopedLock lock(m_mutex);
    return std::sqrt(m_q1/m_i);
  }

 private:
  mutable Mutex m_mutex;
  double m_minv, m_maxv;
  size_t m_i;
  double m_a0, m_a1, m_q0, m_q1;
};

inline std::ostream &operator<<(std::ostream &out, const TestStat &stat) {
  return out <<"min="<< stat.min() <<" max="<< stat.max()
             <<" mean="<< stat.mean() <<" stdev="<< stat.stdev();
}

template <typename FunT>
struct TestFun {
  TestFun(FunT fun, bool proc_stat = false, TestStat *stat = NULL)
    : fun(fun), proc_stat(proc_stat), stat_acc(stat) {}

  void operator()() {
    Stopwatch w;
    fun();
    if (stat_acc || proc_stat)
      std::cout << ThisThread::get_id() <<": ";

    if (stat_acc) {
      stat_acc->add(w.elapsed());
      std::cout <<"Elapsed: "<< w.elapsed() <<"s\n ";
    }
    if (proc_stat) std::cout << System::proc_stat() <<"\n";

    if (stat_acc || proc_stat) std::cout << std::endl;
  }

  FunT fun;
  bool proc_stat;
  TestStat *stat_acc;
};

template <typename FunT>
void serial_run(FunT fun, size_t n, bool proc_stat = false) {
  TestStat stat;

  while (n--) {
    TestFun<FunT> f(fun, proc_stat, &stat);
    f();
  }
  std::cout <<"Elapsed times: "<< stat <<"s\n"<< std::endl;
}

template <typename FunT>
void parallel_run(FunT fun, size_t n, bool proc_stat = false) {
  ThreadGroup pool;
  TestStat stat;

  while (n--) {
    pool.create_thread(TestFun<FunT>(fun, proc_stat, &stat));
  }
  pool.join_all();
  std::cout <<"Elapsed times: "<< stat <<"s\n"<< std::endl;
}

template <typename FunT>
void run_test(FunT fun, bool proc_stat = false, const Properties *props = 0) {
  if (!props) {
    HT_ASSERT(Config::properties);
    props = Config::properties.get();
  }

  size_t threads = props->get_i32("threads", 0);

  if (threads)
    parallel_run(fun, threads, proc_stat);
  else
    serial_run(fun, props->get_i32("repeats", 3), proc_stat);
}

namespace Config {

struct TestPolicy : Config::Policy {
  static void init_options() {
    cmdline_desc().add_options()
      ("repeats,r", i32()->default_value(3), "Number of repeats")
      ("threads,t", i32(), "Number of threads")
      ("num-items,n", i32()->default_value(100000), "Number of items")
      ;
    cmdline_hidden_desc().add_options()
      ("components", strs(), "test components")
      ;
    cmdline_positional_desc().add("components", -1);
  }
};

typedef Cons<TestPolicy, DefaultPolicy> DefaultTestPolicy;

} // namespace Hypertable::Config
} // namespace Hypertable

#endif /* HYPERTABLE_TEST_UTILS_H */
