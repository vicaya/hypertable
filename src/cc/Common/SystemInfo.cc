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

#include "Common/Compat.h"
#include "Common/Config.h"
#include "Common/Logger.h"
#include "Common/SystemInfo.h"
#include "Common/Stopwatch.h"
#include "Common/Mutex.h"

extern "C" {
#include <poll.h>
#include <ncurses.h>
#include <term.h>
#include <sigar.h>
#include <sigar_format.h>
#include <stdlib.h>
}

#define HT_FIELD_NOTIMPL(_field_) (_field_ == (uint64_t)-1)

using namespace Hypertable;

namespace {

RecMutex _mutex;

// for computing cpu percentages
RecMutex _cpu_mutex;
sigar_cpu_t _prev_cpu, *_prev_cpup = NULL;

// default pause (in ms) to calcuate rates
const int DEFAULT_PAUSE = 500;

// for unit conversion to/from MB/GB
const double KiB = 1024.;
const double MiB = KiB * 1024;
const double GiB = MiB * 1024;

// for computing rx/tx rate
RecMutex _net_mutex;
sigar_net_interface_stat_t _prev_net_stat, *_prev_net_statp = NULL;
Stopwatch _net_stat_stopwatch;
const int DEFAULT_NET_STAT_FLAGS =
    SIGAR_NETCONN_CLIENT|SIGAR_NETCONN_SERVER|SIGAR_NETCONN_TCP;

// for computing disk io rate
RecMutex _disk_mutex;
sigar_disk_usage_t _prev_disk_stat, *_prev_disk_statp = NULL;
Stopwatch _disk_stat_stopwatch;

// info/stat singletons
CpuInfo _cpu_info, *_cpu_infop = NULL;
CpuStat _cpu_stat;
MemStat _mem_stat;
DiskStat _disk_stat;
OsInfo _os_info, *_os_infop = NULL;
SwapStat _swap_stat;
NetInfo _net_info, *_net_infop = NULL;
NetStat _net_stat;
ProcInfo _proc_info, *_proc_infop = NULL;
ProcStat _proc_stat;
FsStat _fs_stat;
TermInfo _term_info, *_term_infop = NULL;

// sigar_t singleton to take advantage of the cache
sigar_t *_sigar = NULL;

sigar_t *sigar() {
  if (!_sigar)
    HT_ASSERT(sigar_open(&_sigar) == SIGAR_OK);

  return _sigar;
}

// we don't expect filesystem changes while the process is running
sigar_file_system_list_t _fs_list, *_fs_listp = NULL;

sigar_file_system_list_t &fs_list() {
  if (!_fs_listp) {
    HT_ASSERT(sigar_file_system_list_get(sigar(), &_fs_list) == SIGAR_OK);
    _fs_listp = &_fs_list;
  }
  return _fs_list;
}

bool compute_disk_usage(const char *prefix, sigar_disk_usage_t &u) {
  sigar_disk_usage_t t;
  sigar_file_system_t *fs = fs_list().data, *fs_end = fs + fs_list().number;
  size_t prefix_len = strlen(prefix);
  u.reads = u.writes = u.write_bytes = u.read_bytes = 0;
  size_t num_found = 0;

  for (; fs < fs_end; ++fs) {
    if (strncmp(prefix, fs->dir_name, prefix_len) == 0
        && sigar_disk_usage_get(sigar(), fs->dir_name, &t) == SIGAR_OK) {
      u.reads += t.reads;
      u.writes += t.writes;
      u.read_bytes += t.read_bytes;
      u.write_bytes += t.write_bytes;
      ++num_found;
    }
  }
  if (num_found > 0)
    return true;

  return false;
}

bool compute_fs_usage(const char *prefix, sigar_file_system_usage_t &u) {
  sigar_file_system_usage_t t;
  sigar_file_system_t *fs = fs_list().data, *fs_end = fs + fs_list().number;
  size_t prefix_len = strlen(prefix);
  u.use_percent = 0.;
  u.total = u.free = u.used = u.avail = u.files = u.free_files = 0;
  size_t num_found = 0;

  for (; fs < fs_end; ++fs) {
    if (strncmp(prefix, fs->dir_name, prefix_len) == 0
        && sigar_file_system_usage_get(sigar(), fs->dir_name, &t) == SIGAR_OK) {

      u.use_percent += t.use_percent * t.total; // for weighted mean;
      u.total += t.total;
      u.free += t.free;
      u.used += t.used;
      u.avail += t.avail;
      u.files += t.files;
      u.free_files += t.free_files;
      ++num_found;
    }
  }
  if (num_found > 0) {
    u.use_percent /= u.total;
    return true;
  }
  return false;
}

} // local namespace

namespace Hypertable {

const CpuInfo &System::cpu_info() {
  ScopedRecLock lock(_mutex);

  if (!_cpu_infop)
    _cpu_infop = &_cpu_info.init();

  return _cpu_info;
}

const CpuStat &System::cpu_stat() {
  return _cpu_stat.refresh();
}

const MemStat &System::mem_stat() {
  return _mem_stat.refresh();
}

const DiskStat &System::disk_stat() {
  return _disk_stat.refresh();
}

const OsInfo &System::os_info() {
  ScopedRecLock lock(_mutex);

  if (!_os_infop)
    _os_infop = &_os_info.init();

  return _os_info;
}

const SwapStat &System::swap_stat() {
  return _swap_stat.refresh();
}

const NetInfo &System::net_info() {
  ScopedRecLock lock(_mutex);

  if (!_net_infop)
    _net_infop = &_net_info.init();

  return _net_info;
}

const NetStat &System::net_stat() {
  return _net_stat.refresh();
}

const ProcInfo &System::proc_info() {
  ScopedRecLock lock(_mutex);

  if (!_proc_infop)
    _proc_infop = &_proc_info.init();

  return _proc_info;
}

const ProcStat &System::proc_stat() {
  return _proc_stat.refresh();
}

const FsStat &System::fs_stat() {
  return _fs_stat.refresh();
}

const TermInfo &System::term_info() {
  ScopedRecLock lock(_mutex);

  if (!_term_infop)
    _term_infop = &_term_info.init();

  return _term_info;
}

CpuInfo &CpuInfo::init() {
  ScopedRecLock lock(_mutex);
  sigar_cpu_info_list_t l;

  HT_ASSERT(sigar_cpu_info_list_get(sigar(), &l) == SIGAR_OK);
  sigar_cpu_info_t &s = l.data[0];

  vendor = s.vendor;
  model = s.model;
  mhz = s.mhz;
  cache_size = s.cache_size;
  total_sockets = s.total_sockets;
  total_cores = s.total_cores;
  cores_per_socket = s.cores_per_socket;

  sigar_cpu_info_list_destroy(sigar(), &l);

  return *this;
}

CpuStat &CpuStat::refresh() {
  ScopedRecLock cpu_lock(_cpu_mutex);
  bool need_pause = false;
  {
    ScopedRecLock lock(_mutex); // needed for sigar object

    if (!_prev_cpup) {
      HT_ASSERT(sigar_cpu_get(sigar(), &_prev_cpu) == SIGAR_OK);
      _prev_cpup = &_prev_cpu;
      need_pause = true;
    }
  }
  if (need_pause)
    poll(0, 0, DEFAULT_PAUSE);
  {
    ScopedRecLock lock(_mutex);
    sigar_cpu_t curr;
    HT_ASSERT(sigar_cpu_get(sigar(), &curr) == SIGAR_OK);

    sigar_cpu_perc_t p;
    HT_ASSERT(sigar_cpu_perc_calculate(&_prev_cpu, &curr, &p) == SIGAR_OK);

    user = p.user * 100.;
    sys = p.sys * 100.;
    nice = p.nice * 100.;
    idle = p.idle * 100.;
    wait = p.wait * 100.;
    irq = p.irq * 100.;
    soft_irq = p.soft_irq * 100.;
    stolen = p.stolen * 100.;
    total = p.combined * 100.;

    _prev_cpu = curr;
  }
  return *this;
}

MemStat &MemStat::refresh() {
  ScopedRecLock lock(_mutex);
  sigar_mem_t m;

  HT_ASSERT(sigar_mem_get(sigar(), &m) == SIGAR_OK);

  ram = m.ram;
  total = m.total / MiB;
  used = m.used / MiB;
  free = m.free / MiB;
  actual_used = m.actual_used / MiB;
  actual_free = m.actual_free / MiB;

  return *this;
}

DiskStat &DiskStat::refresh(const char *dir_prefix) {
  ScopedRecLock disk_lock(_disk_mutex);
  bool need_pause = false;
  {
    ScopedRecLock lock(_mutex);

    if ((prefix != dir_prefix || !_prev_disk_statp)
        && compute_disk_usage(dir_prefix, _prev_disk_stat)) {
      HT_DEBUGF("prev_disk_stat: reads=%llu, reads=%llu, read_bytes=%llu "
                "write_bytes=%llu", (Llu)_prev_disk_stat.reads,
                (Llu)_prev_disk_stat.writes, (Llu)_prev_disk_stat.read_bytes,
                (Llu)_prev_disk_stat.write_bytes);
      _prev_disk_statp = &_prev_disk_stat;
      prefix = dir_prefix;
      need_pause = true;
    }
    prefix = dir_prefix;
  }
  if (need_pause)
    poll(0, 0, DEFAULT_PAUSE);
  {
    ScopedRecLock lock(_mutex);

    if (_prev_disk_statp) {
      sigar_disk_usage_t s;

      if (compute_disk_usage(dir_prefix, s)) {
        HT_DEBUGF("curr_disk_stat: reads=%llu, reads=%llu, read_bytes=%llu "
                  "write_bytes=%llu", (Llu)s.reads, (Llu)s.writes,
                  (Llu)s.read_bytes, (Llu)s.write_bytes);
        _disk_stat_stopwatch.stop();
        double elapsed = _disk_stat_stopwatch.elapsed();

        reads_rate = (s.reads - _prev_disk_stat.reads) / elapsed;
        writes_rate = (s.writes - _prev_disk_stat.writes) / elapsed;
        read_rate =
            (s.read_bytes - _prev_disk_stat.read_bytes) / elapsed / MiB;
        write_rate =
            (s.write_bytes - _prev_disk_stat.write_bytes) / elapsed / MiB;

        _prev_disk_stat = s;
        _disk_stat_stopwatch.start();
      }
    }
  }
  return *this;
}

SwapStat &SwapStat::refresh() {
  ScopedRecLock lock(_mutex);
  sigar_swap_t s;

  HT_ASSERT(sigar_swap_get(sigar(), &s) == SIGAR_OK);

  total = s.total / MiB;
  used = s.used / MiB;
  free = s.free / MiB;
  page_in = s.page_in;
  page_out = s.page_out;

  return *this;
}

OsInfo &OsInfo::init() {
  ScopedRecLock lock(_mutex);
  sigar_sys_info_t s;

  HT_ASSERT(sigar_sys_info_get(sigar(), &s) == SIGAR_OK);

  name = s.name;
  version = s.version;

  char *ptr = (char *)version.c_str();
  version_major = (uint16_t)strtol(ptr, &ptr, 10);
  ptr++;
  version_minor = (uint16_t)strtol(ptr, &ptr, 10);
  ptr++;
  version_micro = (uint16_t)strtol(ptr, &ptr, 10);

  arch = s.arch;
  machine = s.machine;
  description = s.description;
  patch_level = s.patch_level;
  vendor = s.vendor;
  vendor_version = s.vendor_version;
  vendor_name = s.vendor_name;

  return *this;
}

NetInfo &NetInfo::init() {
  ScopedRecLock lock(_mutex);
  sigar_net_interface_config_t ifc;
  sigar_net_info_t ni;
  char addrbuf[SIGAR_INET6_ADDRSTRLEN];
  String ifname;

  HT_ASSERT(sigar_net_info_get(sigar(), &ni) == SIGAR_OK);
  host_name = ni.host_name;
  default_gw = ni.default_gateway;

  if (Hypertable::Config::has("Hypertable.Network.Interface"))
    ifname = Hypertable::Config::get_str("Hypertable.Network.Interface");

  if (ifname != "") {
    if (sigar_net_interface_config_get(sigar(), ifname.c_str(), &ifc) == SIGAR_OK) {
      primary_if = ifc.name;
      HT_ASSERT(sigar_net_address_to_string(sigar(), &ifc.address, addrbuf)
		== SIGAR_OK);
      primary_addr = addrbuf;
    }
    else
      HT_FATALF("Unable to find network interface '%s'", ifname.c_str());
  }
  else if (sigar_net_interface_config_primary_get(sigar(), &ifc) == SIGAR_OK) {
    primary_if = ifc.name;
    HT_ASSERT(sigar_net_address_to_string(sigar(), &ifc.address, addrbuf)
              == SIGAR_OK);
    primary_addr = addrbuf;
  }
  if (primary_addr.empty())
    primary_addr = "127.0.0.1";

  return *this;
}

NetStat &NetStat::refresh() {
  ScopedRecLock net_lock(_net_mutex);
  {
    ScopedRecLock lock(_mutex);
    sigar_net_stat_t s;

    if (sigar_net_stat_get(sigar(), &s, DEFAULT_NET_STAT_FLAGS) == SIGAR_OK) {
      tcp_established = s.tcp_states[SIGAR_TCP_ESTABLISHED];
      tcp_listen = s.tcp_states[SIGAR_TCP_LISTEN];
      tcp_time_wait = s.tcp_states[SIGAR_TCP_TIME_WAIT];
      tcp_close_wait = s.tcp_states[SIGAR_TCP_CLOSE_WAIT];
      tcp_idle = s.tcp_states[SIGAR_TCP_IDLE];
    }
    else {
      tcp_established = tcp_listen = tcp_time_wait = tcp_close_wait =
          tcp_idle = 0;
    }
  }
  bool need_pause = false;
  const char *ifname = System::net_info().primary_if.c_str();
  {
    ScopedRecLock lock(_mutex);

    if (!_prev_net_statp) {
      if (sigar_net_interface_stat_get(sigar(), ifname, &_prev_net_stat)
          == SIGAR_OK) {
        _prev_net_statp = &_prev_net_stat;
        _net_stat_stopwatch.start();
        HT_DEBUGF("prev_net_stat: rx_bytes=%llu, tx_bytes=%llu",
                  (Llu)_prev_net_stat.rx_bytes, (Llu)_prev_net_stat.tx_bytes);
        need_pause = true;
      }
      else {
        rx_rate = tx_rate = 0;
      }
    }
  }
  if (need_pause)
    poll(0, 0, DEFAULT_PAUSE);
  {
    ScopedRecLock lock(_mutex);

    if (_prev_net_statp) {
      sigar_net_interface_stat_t curr;

      if (sigar_net_interface_stat_get(sigar(), ifname, &curr) == SIGAR_OK) {
        _net_stat_stopwatch.stop();
        double elapsed = _net_stat_stopwatch.elapsed();
        HT_DEBUGF("curr_net_stat: rx_bytes=%llu, tx_bytes=%llu",
                  (Llu)curr.rx_bytes, (Llu)curr.tx_bytes);

        rx_rate = (curr.rx_bytes - _prev_net_stat.rx_bytes) / elapsed / KiB;
        tx_rate = (curr.tx_bytes - _prev_net_stat.tx_bytes) / elapsed / KiB;

        _prev_net_stat = curr;
        _net_stat_stopwatch.start();
      }
    }
  }
  return *this;
}

ProcInfo &ProcInfo::init() {
  ScopedRecLock lock(_mutex);
  sigar_proc_exe_t exeinfo;
  sigar_proc_args_t arginfo;
  sigar_proc_cred_name_t name;

  memset(&exeinfo, 0, sizeof(exeinfo));
  pid = sigar_pid_get(sigar());
  HT_ASSERT(sigar_proc_exe_get(sigar(), pid, &exeinfo) == SIGAR_OK);
  HT_ASSERT(sigar_proc_args_get(sigar(), pid, &arginfo) == SIGAR_OK);
  HT_ASSERT(sigar_proc_cred_name_get(sigar(), pid, &name) == SIGAR_OK);

  user = name.user;
  exe = exeinfo.name;
  cwd = exeinfo.cwd;
  root = exeinfo.root;

  for (size_t i = 0; i < arginfo.number; ++i)
    args.push_back(arginfo.data[i]);

  sigar_proc_args_destroy(sigar(), &arginfo);

  return *this;
}

ProcStat &ProcStat::refresh() {
  ScopedRecLock lock(_mutex);
  sigar_proc_cpu_t c;
  sigar_proc_mem_t m;

  sigar_pid_t pid = sigar_pid_get(sigar());
  HT_ASSERT(sigar_proc_cpu_get(sigar(), pid, &c) == SIGAR_OK);
  HT_ASSERT(sigar_proc_mem_get(sigar(), pid, &m) == SIGAR_OK);

  cpu_user = c.user;
  cpu_sys = c.sys;
  cpu_total = c.total;
  cpu_pct = c.percent * 100.;
  vm_size = m.size / MiB;
  vm_resident = m.resident / MiB;
  vm_share = HT_FIELD_NOTIMPL(m.share) ? 0 : m.share / MiB;
  minor_faults = HT_FIELD_NOTIMPL(m.minor_faults) ? 0 : m.minor_faults;
  major_faults = HT_FIELD_NOTIMPL(m.major_faults) ? 0 : m.major_faults;
  page_faults = HT_FIELD_NOTIMPL(m.page_faults) ? 0 : m.page_faults;;

  return *this;
}

FsStat &FsStat::refresh(const char *dir_prefix) {
  ScopedRecLock lock(_mutex);
  sigar_file_system_usage_t u;
  prefix = dir_prefix;

  if (compute_fs_usage(dir_prefix, u)) {
    use_pct = u.use_percent * 100.;
    total = u.total / MiB; // u.total already in KB
    free = u.free / MiB;
    used = u.used / MiB;
    avail = u.avail / MiB;
    files = u.files;
    free_files = u.free_files;
  }
  return *this;
}

TermInfo &TermInfo::init() {
  ScopedRecLock lock(_mutex);
  int err;

  if (setupterm(0,  1, &err) != ERR) {
    term = getenv("TERM");
    num_lines = lines;
    num_cols = columns;
  }
  else {
    term = "dumb";
    num_lines = 24;
    num_cols = 80;
  }
  return *this;
}

const char *system_info_lib_version() {
  sigar_version_t *v = sigar_version_get();
  return v->description;
}

std::ostream &system_info_lib_version(std::ostream &out) {
  sigar_version_t *v = sigar_version_get();
  out <<"{SigarVersion: build_date='"<< v->build_date <<"' scm_revision='"
      << v->scm_revision <<"'\n version='"<< v->version <<"' archname='"
      << v->archname <<"'\n archlib='"<< v->archlib <<"' binname='"
      << v->binname <<"'\n description='"<< v->description <<"'\n major="
      << v->major <<" minor="<< v->minor <<" maint="<< v->maint <<" build="
      << v->build <<'}';
  return out;
}

std::ostream &operator<<(std::ostream &out, const CpuInfo &i) {
  out <<"{CpuInfo: vendor='"<< i.vendor <<"' model='"<< i.model
      <<"' mhz="<< i.mhz <<" cache_size="<< i.cache_size
      <<"\n total_sockets="<< i.total_sockets <<" total_cores="<< i.total_cores
      <<" cores_per_socket="<< i.cores_per_socket <<'}';
  return out;
}

std::ostream &operator<<(std::ostream &out, const CpuStat &s) {
  out <<"{CpuStat: user="<< s.user <<" sys="<< s.sys <<" nice="<< s.nice
      <<" idle="<< s.idle <<" wait="<< s.wait <<"\n irq="<< s.irq
      <<" soft_irq="<< s.soft_irq <<" stolen="<< s.stolen
      <<" total="<< s.total <<'}';
  return out;
}

std::ostream &operator<<(std::ostream &out, const MemStat &s) {
  out <<"{MemStat: ram="<< s.ram <<" total="<< s.total <<" used="<< s.used
      <<" free="<< s.free <<"\n actual_used="<< s.actual_used
      <<" actual_free="<< s.actual_free <<'}';
  return out;
}

std::ostream &operator<<(std::ostream &out, const DiskStat &s) {
  out <<"{DiskStat: prefix='"<< s.prefix <<"' reads_rate="<< s.reads_rate
      <<" writes_rate="<< s.writes_rate <<"\n read_rate=" << s.read_rate
      <<" write_rate="<< s.write_rate <<'}';
  return out;
}

std::ostream &operator<<(std::ostream &out, const OsInfo &i) {
  out <<"{OsInfo: name="<< i.name <<" version="<< i.version <<" arch="<< i.arch
      <<" machine="<< i.machine <<"\n description='"<< i.description
      <<"' patch_level="<< i.patch_level <<"\n vendor="<< i.vendor
      <<" vendor_version="<< i.vendor_version <<" vendor_name="<< i.vendor_name
      <<" code_name="<< i.code_name <<'}';
  return out;
}

std::ostream &operator<<(std::ostream &out, const SwapStat &s) {
  out <<"{SwapStat: total="<< s.total <<" used="<< s.used <<" free="<< s.free
      <<" page_in="<< s.page_in <<" page_out="<< s.page_out <<'}';
  return out;
}

std::ostream &operator<<(std::ostream &out, const NetInfo &i) {
  out <<"{NetInfo: host_name="<< i.host_name <<" primary_if="
      << i.primary_if <<"\n primary_addr="<< i.primary_addr
      <<" default_gw="<< i.default_gw <<'}';
  return out;
}

std::ostream &operator<<(std::ostream &out, const NetStat &s) {
  out <<"{NetStat: rx_rate="<< s.rx_rate <<" tx_rate="<< s.tx_rate
      <<" tcp_established="<< s.tcp_established <<" tcp_listen="
      << s.tcp_listen <<"\n tcp_time_wait="<< s.tcp_time_wait
      <<" tcp_close_wait="<< s.tcp_close_wait <<'}';
  return out;
}

std::ostream &operator<<(std::ostream &out, const ProcInfo &i) {
  out <<"{ProcInfo: pid="<< i.pid <<" user="<< i.user <<" exe='"<< i.exe
      <<"'\n cwd='"<< i.cwd <<"' root='"<< i.root <<"'\n args=[";

  foreach(const String &arg, i.args)
    out <<"'"<< arg <<"', ";

  out <<"]}";
  return out;
}

std::ostream &operator<<(std::ostream &out, const ProcStat &s) {
  out <<"{ProcStat: cpu_user="<< s.cpu_user <<" cpu_sys="<< s.cpu_sys
      <<" cpu_total="<< s.cpu_total <<" cpu_pct="<< s.cpu_pct
      <<"\n vm_size="<< s.vm_size <<" vm_resident="<< s.vm_resident
      <<" vm_share="<< s.vm_share <<"\n major_faults="<< s.major_faults
      <<" minor_faults="<< s.minor_faults <<" page_faults="<< s.page_faults
      <<'}';
  return out;
}

std::ostream &operator<<(std::ostream &out, const FsStat &s) {
  out <<"{FsStat: prefix='"<< s.prefix <<"' total="<< s.total
      <<" free="<< s.free <<" used="<< s.used <<"\n avail="<< s.avail
      <<" use_pct="<< s.use_pct <<'}';
  return out;
}

std::ostream &operator<<(std::ostream &out, const TermInfo &i) {
  out <<"{TermInfo: term='"<< i.term <<"' num_lines="<< i.num_lines
      <<" num_cols="<< i.num_cols <<'}';
  return out;
}

} // namespace Hypertable
