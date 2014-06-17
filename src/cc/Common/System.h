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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_SYSTEM_H
#define HYPERTABLE_SYSTEM_H

#include <boost/random.hpp>
#include "Common/Version.h"
#include "Common/Mutex.h"
#include "Common/String.h"

namespace Hypertable {

  class CpuInfo;
  class CpuStat;
  class LoadAvgStat;
  class MemStat;
  class DiskStat;
  class OsInfo;
  class SwapStat;
  class NetInfo;
  class NetStat;
  class ProcInfo;
  class ProcStat;
  class FsStat;
  class TermInfo;

  class System {
  public:
    // must be inlined to do proper version check
    static inline void initialize(const String &install_directory = String()) {
      ScopedLock lock(ms_mutex);

      if (ms_initialized)
        return;

      check_version();
      _init(install_directory);
      ms_initialized = true;
    }

    static String locate_install_dir(const char *argv0);
    static String _locate_install_dir(const char *argv0);

    static String install_dir;
    static String exe_name;

    static int32_t get_processor_count();
    static int32_t get_pid();

    static uint32_t rand32() { return ms_rng(); }
    static uint64_t rand64() { return (uint64_t)rand32() << 32 | rand32(); }

    // system info objects
    static const CpuInfo &cpu_info();
    static const CpuStat &cpu_stat();
    static const MemStat &mem_stat();
    static const DiskStat &disk_stat();
    static const OsInfo &os_info();
    static const SwapStat &swap_stat();
    static const NetInfo &net_info();
    static const NetStat &net_stat();
    static const ProcInfo &proc_info();
    static const ProcStat &proc_stat();
    static const FsStat &fs_stat();
    static const TermInfo &term_info();
    static const LoadAvgStat &loadavg_stat();

  private:
    static void _init(const String &install_directory);

    static bool ms_initialized;
    static Mutex ms_mutex;
    static boost::mt19937 ms_rng;
  };

} // namespace Hypertable

#endif // HYPERTABLE_SYSTEM_H
