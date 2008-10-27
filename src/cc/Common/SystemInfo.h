/**
 * Copyright (C) 2007 Luke Lu (Zvents, Inc.)
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

#ifndef HYPERTABLE_SYSTEMINFO_H
#define HYPERTABLE_SYSTEMINFO_H

#include <iosfwd>
#include "Common/System.h"
#include "Common/InetAddr.h"

namespace Hypertable {

  struct CpuInfo {
    CpuInfo &init();

    String vendor;
    String model;
    int mhz;
    uint32_t cache_size;
    int total_sockets;
    int total_cores;
    int cores_per_socket;
  };

  struct CpuStat {
    CpuStat &refresh();

    // system wide cpu time in percentage
    double user;
    double sys;
    double nice;
    double idle;
    double wait;
    double irq;
    double soft_irq;
    double stolen;      // for virtualized env (Linux 2.6.11+)
    double total;
  };

  struct MemStat {
    MemStat &refresh();

    // memory usage in MB
    double ram;
    double total;
    double used;
    double free;
    double actual_used; // excluding kernel buffers/caches
    double actual_free; // including kernel buffers/caches
  };

  struct DiskStat {
    DiskStat &refresh(const char *dir_prefix = "/");

    String prefix;
    // aggregate io ops rate
    double reads_rate;
    double writes_rate;

    // aggreate transfer rate in MB/s
    double read_rate;
    double write_rate;
  };

  struct SwapStat {
    SwapStat &refresh();

    // aggregate in MB
    double total;
    double used;
    double free;

    uint64_t page_in;
    uint64_t page_out;
  };

  struct NetInfo {
    NetInfo &init();

    String host_name;
    String primary_if;
    String primary_addr;
    String default_gw;
  };

  struct NetStat {
    NetStat &refresh();

    int32_t tcp_established;
    int32_t tcp_listen;
    int32_t tcp_time_wait;
    int32_t tcp_close_wait;    // often indicating bugs
    int32_t tcp_idle;

    // transfer rate in KB/s
    double rx_rate;   // receiving rate only primary interface for now
    double tx_rate;   // transmitting rate...
  };

  struct OsInfo {
    OsInfo &init();

    String name;        // canonical system name
    String version;
    String arch;
    String machine;     // more specific than arch (e.g. i686 vs i386)
    String description;
    String patch_level;
    String vendor;
    String vendor_version;
    String vendor_name;
    String code_name;
  };

  struct ProcInfo {
    ProcInfo &init();

    int64_t pid;
    String user;
    String exe;
    String cwd;
    String root;
    std::vector<String> args;
  };

  struct ProcStat {
    ProcStat &refresh();

    // proc cpu time in ms
    uint64_t cpu_user;
    uint64_t cpu_sys;
    uint64_t cpu_total;
    double cpu_pct;

    // proc (virtual) memory usage in MB
    double vm_size;
    double vm_resident;
    double vm_share;
    uint64_t minor_faults;
    uint64_t major_faults;
    uint64_t page_faults;
  };

  struct FsStat {
    FsStat &refresh(const char *prefix = "/");

    // aggregate file system usage in GB
    String prefix;
    double total;
    double free;
    double used;
    double use_pct;
    double avail; // available to non-root users

    // aggregate files/inodes
    uint64_t files;
    uint64_t free_files; // free inodes
  };

  std::ostream &operator<<(std::ostream &, const CpuInfo &);
  std::ostream &operator<<(std::ostream &, const CpuStat &);
  std::ostream &operator<<(std::ostream &, const MemStat &);
  std::ostream &operator<<(std::ostream &, const DiskStat &);
  std::ostream &operator<<(std::ostream &, const OsInfo &);
  std::ostream &operator<<(std::ostream &, const SwapStat &);
  std::ostream &operator<<(std::ostream &, const NetInfo &);
  std::ostream &operator<<(std::ostream &, const NetStat &);
  std::ostream &operator<<(std::ostream &, const ProcInfo &);
  std::ostream &operator<<(std::ostream &, const ProcStat &);
  std::ostream &operator<<(std::ostream &, const FsStat &);

  const char *system_info_lib_version();
  std::ostream &system_info_lib_version(std::ostream &);


} // namespace Hypertable

#endif // HYPERTABLE_SYSTEMINFO_H
