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
    bool operator==(const CpuInfo &other) const;
    bool operator!=(const CpuInfo &other) const {
      return !(*this == other);
    }

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
    bool operator==(const CpuStat &other) const;
    bool operator!=(const CpuStat &other) const {
      return !(*this == other);
    }

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

  /**
   * Reports loadavg normalized over #cores
   */
  struct LoadAvgStat {
    LoadAvgStat &refresh();
    bool operator==(const LoadAvgStat &other) const;
    bool operator!=(const LoadAvgStat &other) const {
      return !(*this == other);
    }

    double loadavg[3];
  };

  struct MemStat {
    MemStat &refresh();
    bool operator==(const MemStat &other) const;
    bool operator!=(const MemStat &other) const {
      return !(*this == other);
    }

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
    bool operator==(const DiskStat &other) const;
    bool operator!=(const DiskStat &other) const {
      return !(*this == other);
    }

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
    bool operator==(const SwapStat &other) const;
    bool operator!=(const SwapStat &other) const {
      return !(*this == other);
    }

    // aggregate in MB
    double total;
    double used;
    double free;

    uint64_t page_in;
    uint64_t page_out;
  };

  struct NetInfo {
    NetInfo &init();
    bool operator==(const NetInfo &other) const;
    bool operator!=(const NetInfo &other) const {
      return !(*this == other);
    }

    String host_name;
    String primary_if;
    String primary_addr;
    String default_gw;
  };

  struct NetStat {
    NetStat &refresh();
    bool operator==(const NetStat &other) const;
    bool operator!=(const NetStat &other) const {
      return !(*this == other);
    }

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
    bool operator==(const OsInfo &other) const;
    bool operator!=(const OsInfo &other) const {
      return !(*this == other);
    }

    String name;        // canonical system name
    String version;
    uint16_t version_major;
    uint16_t version_minor;
    uint16_t version_micro;
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
    bool operator==(const ProcInfo &other) const;
    bool operator!=(const ProcInfo &other) const {
      return !(*this == other);
    }

    int64_t pid;
    String user;
    String exe;
    String cwd;
    String root;
    std::vector<String> args;
  };

  struct ProcStat {
    ProcStat &refresh();
    bool operator==(const ProcStat &other) const;
    bool operator!=(const ProcStat &other) const {
      return !(*this == other);
    }

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
    bool operator==(const FsStat &other) const;
    bool operator!=(const FsStat &other) const {
      return !(*this == other);
    }

    // aggregate file system usage in GB
    String prefix;
    double use_pct;
    uint64_t total;
    uint64_t free;
    uint64_t used;
    uint64_t avail; // available to non-root users

    // aggregate files/inodes
    uint64_t files;
    uint64_t free_files; // free inodes
  };

  struct TermInfo {
    TermInfo &init();
    bool operator==(const TermInfo &other) const;
    bool operator!=(const TermInfo &other) const {
      return !(*this == other);
    }

    String term;
    int num_lines;
    int num_cols;
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
  std::ostream &operator<<(std::ostream &, const TermInfo &);

  const char *system_info_lib_version();
  std::ostream &system_info_lib_version(std::ostream &);


} // namespace Hypertable

#endif // HYPERTABLE_SYSTEMINFO_H
