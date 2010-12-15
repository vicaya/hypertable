/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License.
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
#include "Common/Logger.h"
#include "Common/Serialization.h"

#include <boost/algorithm/string.hpp>

#include "StatsSystem.h"

using namespace Hypertable;

namespace {

  enum Group {
    CPUINFO_GROUP  = 0,
    CPU_GROUP      = 1,
    LOADAVG_GROUP  = 2,
    MEMORY_GROUP   = 3,
    DISK_GROUP     = 4,
    SWAP_GROUP     = 5,
    NETINFO_GROUP  = 6,
    NET_GROUP      = 7,
    OSINFO_GROUP   = 8,
    PROCINFO_GROUP = 9,
    PROC_GROUP     = 10,
    FS_GROUP       = 11,
    TERMINFO_GROUP = 12
  };

}

StatsSystem::StatsSystem(int32_t categories) 
  : StatsSerializable(SYSTEM, 0), m_categories(categories) {
  group_count = 0;
  add_categories(categories);
  HT_ASSERT((categories&(DISK|FS)) == 0);
}

StatsSystem::StatsSystem(int32_t categories, std::vector<String> &dirs)
  : StatsSerializable(SYSTEM, 0), m_categories(categories) {
  group_count = 0;
  add_categories(categories, dirs);
  HT_ASSERT((categories&(DISK|FS)) != 0);
}


void StatsSystem::add_categories(int32_t categories) {
  m_categories |= categories;
  if (categories & CPUINFO) {
    group_ids[group_count++] = CPUINFO_GROUP;
    cpu_info.init();
  }
  if (categories & NETINFO) {
    group_ids[group_count++] = NETINFO_GROUP;
    net_info.init();
  }
  if (categories & OSINFO) {
    group_ids[group_count++] = OSINFO_GROUP;
    os_info.init();
  }
  if (categories & PROCINFO) {
    group_ids[group_count++] = PROCINFO_GROUP;
    proc_info.init();
  }
  if (categories & TERMINFO) {
    group_ids[group_count++] = TERMINFO_GROUP;
    term_info.init();
  }
  if (categories & CPU) {
    group_ids[group_count++] = CPU_GROUP;
    cpu_stat.refresh();
  }
  if (categories & LOADAVG) {
    group_ids[group_count++] = LOADAVG_GROUP;
    loadavg_stat.refresh();
  }
  if (categories & MEMORY) {
    group_ids[group_count++] = MEMORY_GROUP;
    mem_stat.refresh();
  }
  if (categories & SWAP) {
    group_ids[group_count++] = SWAP_GROUP;
    swap_stat.refresh();
  }
  if (categories & NET) {
    group_ids[group_count++] = NET_GROUP;
    net_stat.refresh();
  }
  if (categories & PROC) {
    group_ids[group_count++] = PROC_GROUP;
    proc_stat.refresh();
  }
}

void StatsSystem::add_categories(int32_t categories, std::vector<String> &dirs) {
  add_categories(categories);

  // strip trailing '/'
  for (size_t i=0; i<dirs.size(); i++)
    boost::trim_right_if(dirs[i], boost::is_any_of("/"));

  if (categories & DISK) {
    group_ids[group_count++] = DISK_GROUP;
    disk_stat.resize(dirs.size());
    for (size_t i=0; i<dirs.size(); i++)
      disk_stat[i].prefix = dirs[i];
  }
  if (categories & FS) {
    group_ids[group_count++] = FS_GROUP;
    fs_stat.resize(dirs.size());
    for (size_t i=0; i<dirs.size(); i++)
      fs_stat[i].prefix = dirs[i];
  }
}

void StatsSystem::refresh() {
  if (m_categories & CPU)
    cpu_stat.refresh();
  if (m_categories & LOADAVG)
    loadavg_stat.refresh();
  if (m_categories & MEMORY)
    mem_stat.refresh();
  if (m_categories & SWAP)
    swap_stat.refresh();
  if (m_categories & NET)
    net_stat.refresh();
  if (m_categories & PROC)
    proc_stat.refresh();
  if (m_categories & DISK) {
    for (size_t i=0; i<disk_stat.size(); i++)
      disk_stat[i].refresh(disk_stat[i].prefix.c_str());
  }
  if (m_categories & FS) {
    for (size_t i=0; i<fs_stat.size(); i++)
      fs_stat[i].refresh(fs_stat[i].prefix.c_str());
  }
}

bool StatsSystem::operator==(const StatsSystem &other) const {
  if (StatsSerializable::operator!=(other))
    return false;
  if (m_categories != other.m_categories)
    return false;
  if ((m_categories & CPUINFO) &&
      cpu_info != other.cpu_info)
    return false;
  if ((m_categories & CPU) &&
      cpu_stat != other.cpu_stat)
    return false;
  if ((m_categories & LOADAVG) &&
      loadavg_stat != other.loadavg_stat)
    return false;
  if ((m_categories & MEMORY) &&
      mem_stat != other.mem_stat)
    return false;
  if ((m_categories & SWAP) &&
      swap_stat != other.swap_stat)
    return false;
  if ((m_categories & NETINFO) &&
      net_info != other.net_info)
    return false;
  if ((m_categories & NET) &&
      net_stat != other.net_stat)
    return false;
  if ((m_categories & OSINFO) &&
      os_info != other.os_info)
    return false;
  if ((m_categories & PROCINFO) &&
      proc_info != other.proc_info)
    return false;
  if ((m_categories & PROC) &&
      proc_stat != other.proc_stat)
    return false;
  if ((m_categories & TERMINFO) &&  
      term_info != other.term_info)
    return false;
  if (m_categories & DISK) {
    if (disk_stat.size() != other.disk_stat.size())
      return false;
    for (size_t i=0; i<disk_stat.size(); i++) {
      if (disk_stat[i] != other.disk_stat[i])
        return false;
    }
  }
  if (m_categories & FS) {
    if (fs_stat.size() != other.fs_stat.size())
      return false;
    for (size_t i=0; i<fs_stat.size(); i++) {
      if (fs_stat[i] != other.fs_stat[i])
        return false;
    }
  }
  return true;
}

size_t StatsSystem::encoded_length_group(int group) const {
  if (group == CPUINFO_GROUP) {
    return Serialization::encoded_length_vstr(cpu_info.vendor) + \
      Serialization::encoded_length_vstr(cpu_info.model) + 5*4;
  }
  else if (group == CPU_GROUP) {
    return 9*Serialization::encoded_length_double();
  }
  else if (group == LOADAVG_GROUP) {
    return 3*Serialization::encoded_length_double();
  }
  else if (group == MEMORY_GROUP) {
    return 6*Serialization::encoded_length_double();
  }
  else if (group == DISK_GROUP) {
    size_t len = Serialization::encoded_length_vi32(disk_stat.size());
    for (size_t i=0; i<disk_stat.size(); i++) {
      len += Serialization::encoded_length_vstr(disk_stat[i].prefix) + \
        4*Serialization::encoded_length_double();
    }
    return len;
  }
  else if (group == SWAP_GROUP) {
    return (3*Serialization::encoded_length_double()) + 16;
  }
  else if (group == NETINFO_GROUP) {
    return Serialization::encoded_length_vstr(net_info.host_name) + \
      Serialization::encoded_length_vstr(net_info.primary_if) + \
      Serialization::encoded_length_vstr(net_info.primary_addr) + \
      Serialization::encoded_length_vstr(net_info.default_gw);
  }
  else if (group == NET_GROUP) {
    return (5*4) + (2*Serialization::encoded_length_double());
  }
  else if (group == OSINFO_GROUP) {
    return 6 + Serialization::encoded_length_vstr(os_info.name) + \
      Serialization::encoded_length_vstr(os_info.version) + \
      Serialization::encoded_length_vstr(os_info.arch) + \
      Serialization::encoded_length_vstr(os_info.machine) + \
      Serialization::encoded_length_vstr(os_info.description) + \
      Serialization::encoded_length_vstr(os_info.patch_level) + \
      Serialization::encoded_length_vstr(os_info.vendor) + \
      Serialization::encoded_length_vstr(os_info.vendor_version) + \
      Serialization::encoded_length_vstr(os_info.vendor_name) + \
      Serialization::encoded_length_vstr(os_info.code_name);
  }
  else if (group == PROCINFO_GROUP) {
    size_t len = 8 + Serialization::encoded_length_vstr(proc_info.user) + \
      Serialization::encoded_length_vstr(proc_info.exe) + \
      Serialization::encoded_length_vstr(proc_info.cwd) + \
      Serialization::encoded_length_vstr(proc_info.root);
    len += Serialization::encoded_length_vi32(proc_info.args.size());
    for (size_t i=0; i<proc_info.args.size(); i++)
      len += Serialization::encoded_length_vstr(proc_info.args[i]);
    return len;
  }
  else if (group == PROC_GROUP) {
    return (6*8) + (4*Serialization::encoded_length_double());
  }
  else if (group == FS_GROUP) {
    size_t len = Serialization::encoded_length_vi32(fs_stat.size());
    for (size_t i=0; i<fs_stat.size(); i++) {
      len += (6*8) + Serialization::encoded_length_double() + \
        Serialization::encoded_length_vstr(fs_stat[i].prefix);
    }
    return len;
  }
  else if (group == TERMINFO_GROUP) {
    return (2*4) + Serialization::encoded_length_vstr(term_info.term);
  }
  else
    HT_FATALF("Invalid group number (%d)", group);
  return 0;
}

void StatsSystem::encode_group(int group, uint8_t **bufp) const {
  if (group == CPUINFO_GROUP) {
    Serialization::encode_vstr(bufp, cpu_info.vendor);
    Serialization::encode_vstr(bufp, cpu_info.model);
    Serialization::encode_i32(bufp, cpu_info.mhz);
    Serialization::encode_i32(bufp, cpu_info.cache_size);
    Serialization::encode_i32(bufp, cpu_info.total_sockets);
    Serialization::encode_i32(bufp, cpu_info.total_cores);
    Serialization::encode_i32(bufp, cpu_info.cores_per_socket);
  }
  else if (group == CPU_GROUP) {
    Serialization::encode_double(bufp, cpu_stat.user);
    Serialization::encode_double(bufp, cpu_stat.sys);
    Serialization::encode_double(bufp, cpu_stat.nice);
    Serialization::encode_double(bufp, cpu_stat.idle);
    Serialization::encode_double(bufp, cpu_stat.wait);
    Serialization::encode_double(bufp, cpu_stat.irq);
    Serialization::encode_double(bufp, cpu_stat.soft_irq);
    Serialization::encode_double(bufp, cpu_stat.stolen);
    Serialization::encode_double(bufp, cpu_stat.total);
  }
  else if (group == LOADAVG_GROUP) {
    Serialization::encode_double(bufp, loadavg_stat.loadavg[0]);
    Serialization::encode_double(bufp, loadavg_stat.loadavg[1]);
    Serialization::encode_double(bufp, loadavg_stat.loadavg[2]);
  }
  else if (group == MEMORY_GROUP) {
    Serialization::encode_double(bufp, mem_stat.ram);
    Serialization::encode_double(bufp, mem_stat.total);
    Serialization::encode_double(bufp, mem_stat.used);
    Serialization::encode_double(bufp, mem_stat.free);
    Serialization::encode_double(bufp, mem_stat.actual_used);
    Serialization::encode_double(bufp, mem_stat.actual_free);
  }
  else if (group == DISK_GROUP) {
    Serialization::encode_vi32(bufp, disk_stat.size());
    for (size_t i=0; i<disk_stat.size(); i++) {
      Serialization::encode_vstr(bufp, disk_stat[i].prefix);
      Serialization::encode_double(bufp, disk_stat[i].reads_rate);
      Serialization::encode_double(bufp, disk_stat[i].writes_rate);
      Serialization::encode_double(bufp, disk_stat[i].read_rate);
      Serialization::encode_double(bufp, disk_stat[i].write_rate);
    }
  }
  else if (group == SWAP_GROUP) {
    Serialization::encode_double(bufp, swap_stat.total);
    Serialization::encode_double(bufp, swap_stat.used);
    Serialization::encode_double(bufp, swap_stat.free);
    Serialization::encode_i64(bufp, swap_stat.page_in);
    Serialization::encode_i64(bufp, swap_stat.page_out);
  }
  else if (group == NETINFO_GROUP) {
    Serialization::encode_vstr(bufp, net_info.host_name);
    Serialization::encode_vstr(bufp, net_info.primary_if);
    Serialization::encode_vstr(bufp, net_info.primary_addr);
    Serialization::encode_vstr(bufp, net_info.default_gw);
  }
  else if (group == NET_GROUP) {
    Serialization::encode_i32(bufp, net_stat.tcp_established);
    Serialization::encode_i32(bufp, net_stat.tcp_listen);
    Serialization::encode_i32(bufp, net_stat.tcp_time_wait);
    Serialization::encode_i32(bufp, net_stat.tcp_close_wait);
    Serialization::encode_i32(bufp, net_stat.tcp_idle);
    Serialization::encode_double(bufp, net_stat.rx_rate);
    Serialization::encode_double(bufp, net_stat.tx_rate);
  }
  else if (group == OSINFO_GROUP) {
    Serialization::encode_vstr(bufp, os_info.name);
    Serialization::encode_vstr(bufp, os_info.version);
    Serialization::encode_i16(bufp, os_info.version_major);
    Serialization::encode_i16(bufp, os_info.version_minor);
    Serialization::encode_i16(bufp, os_info.version_micro);
    Serialization::encode_vstr(bufp, os_info.arch);
    Serialization::encode_vstr(bufp, os_info.machine);
    Serialization::encode_vstr(bufp, os_info.description);
    Serialization::encode_vstr(bufp, os_info.patch_level);
    Serialization::encode_vstr(bufp, os_info.vendor);
    Serialization::encode_vstr(bufp, os_info.vendor_version);
    Serialization::encode_vstr(bufp, os_info.vendor_name);
    Serialization::encode_vstr(bufp, os_info.code_name);
  }
  else if (group == PROCINFO_GROUP) {
    Serialization::encode_i64(bufp, proc_info.pid);
    Serialization::encode_vstr(bufp, proc_info.user);
    Serialization::encode_vstr(bufp, proc_info.exe);
    Serialization::encode_vstr(bufp, proc_info.cwd);
    Serialization::encode_vstr(bufp, proc_info.root);
    Serialization::encode_vi32(bufp, proc_info.args.size());
    for (size_t i=0; i<proc_info.args.size(); i++)
      Serialization::encode_vstr(bufp, proc_info.args[i]);
  }
  else if (group == PROC_GROUP) {
    Serialization::encode_i64(bufp, proc_stat.cpu_user);
    Serialization::encode_i64(bufp, proc_stat.cpu_sys);
    Serialization::encode_i64(bufp, proc_stat.cpu_total);
    Serialization::encode_double(bufp, proc_stat.cpu_pct);
    Serialization::encode_double(bufp, proc_stat.vm_size);
    Serialization::encode_double(bufp, proc_stat.vm_resident);
    Serialization::encode_double(bufp, proc_stat.vm_share);
    Serialization::encode_i64(bufp, proc_stat.minor_faults);
    Serialization::encode_i64(bufp, proc_stat.major_faults);
    Serialization::encode_i64(bufp, proc_stat.page_faults);
  }
  else if (group == FS_GROUP) {
    Serialization::encode_vi32(bufp, fs_stat.size());
    for (size_t i=0; i<fs_stat.size(); i++) {
      Serialization::encode_vstr(bufp, fs_stat[i].prefix);
      Serialization::encode_double(bufp, fs_stat[i].use_pct);
      Serialization::encode_i64(bufp, fs_stat[i].total);
      Serialization::encode_i64(bufp, fs_stat[i].free);
      Serialization::encode_i64(bufp, fs_stat[i].used);
      Serialization::encode_i64(bufp, fs_stat[i].avail);
      Serialization::encode_i64(bufp, fs_stat[i].files);
      Serialization::encode_i64(bufp, fs_stat[i].free_files);
    }
  }
  else if (group == TERMINFO_GROUP) {
    Serialization::encode_vstr(bufp, term_info.term);
    Serialization::encode_i32(bufp, term_info.num_lines);
    Serialization::encode_i32(bufp, term_info.num_cols);
  }
  else
    HT_FATALF("Invalid group number (%d)", group);
}

void StatsSystem::decode_group(int group, uint16_t len, const uint8_t **bufp, size_t *remainp) {
  if (group == CPUINFO_GROUP) {
    cpu_info.vendor = Serialization::decode_vstr(bufp, remainp);
    cpu_info.model = Serialization::decode_vstr(bufp, remainp);
    cpu_info.mhz = Serialization::decode_i32(bufp, remainp);
    cpu_info.cache_size = Serialization::decode_i32(bufp, remainp);
    cpu_info.total_sockets = Serialization::decode_i32(bufp, remainp);
    cpu_info.total_cores = Serialization::decode_i32(bufp, remainp);
    cpu_info.cores_per_socket = Serialization::decode_i32(bufp, remainp);
    m_categories |= CPUINFO;
  }
  else if (group == CPU_GROUP) {
    cpu_stat.user = Serialization::decode_double(bufp, remainp);
    cpu_stat.sys = Serialization::decode_double(bufp, remainp);
    cpu_stat.nice = Serialization::decode_double(bufp, remainp);
    cpu_stat.idle = Serialization::decode_double(bufp, remainp);
    cpu_stat.wait = Serialization::decode_double(bufp, remainp);
    cpu_stat.irq = Serialization::decode_double(bufp, remainp);
    cpu_stat.soft_irq = Serialization::decode_double(bufp, remainp);
    cpu_stat.stolen = Serialization::decode_double(bufp, remainp);
    cpu_stat.total = Serialization::decode_double(bufp, remainp);
    m_categories |= CPU;
  }
  else if (group == LOADAVG_GROUP) {
    loadavg_stat.loadavg[0] = Serialization::decode_double(bufp, remainp);
    loadavg_stat.loadavg[1] = Serialization::decode_double(bufp, remainp);
    loadavg_stat.loadavg[2] = Serialization::decode_double(bufp, remainp);
    m_categories |= LOADAVG;
  }
  else if (group == MEMORY_GROUP) {
    mem_stat.ram = Serialization::decode_double(bufp, remainp);
    mem_stat.total = Serialization::decode_double(bufp, remainp);
    mem_stat.used = Serialization::decode_double(bufp, remainp);
    mem_stat.free = Serialization::decode_double(bufp, remainp);
    mem_stat.actual_used = Serialization::decode_double(bufp, remainp);
    mem_stat.actual_free = Serialization::decode_double(bufp, remainp);
    m_categories |= MEMORY;
  }
  else if (group == DISK_GROUP) {
    size_t count = Serialization::decode_vi32(bufp, remainp);
    disk_stat.resize(count);
    for (size_t i=0; i<count; i++) {
      disk_stat[i].prefix = Serialization::decode_vstr(bufp, remainp);
      disk_stat[i].reads_rate = Serialization::decode_double(bufp, remainp);
      disk_stat[i].writes_rate = Serialization::decode_double(bufp, remainp);
      disk_stat[i].read_rate = Serialization::decode_double(bufp, remainp);
      disk_stat[i].write_rate = Serialization::decode_double(bufp, remainp);
    }
    m_categories |= DISK;
  }
  else if (group == SWAP_GROUP) {
    swap_stat.total = Serialization::decode_double(bufp, remainp);
    swap_stat.used = Serialization::decode_double(bufp, remainp);
    swap_stat.free = Serialization::decode_double(bufp, remainp);
    swap_stat.page_in = Serialization::decode_i64(bufp, remainp);
    swap_stat.page_out = Serialization::decode_i64(bufp, remainp);
    m_categories |= SWAP;
  }
  else if (group == NETINFO_GROUP) {
    net_info.host_name = Serialization::decode_vstr(bufp, remainp);
    net_info.primary_if = Serialization::decode_vstr(bufp, remainp);
    net_info.primary_addr = Serialization::decode_vstr(bufp, remainp);
    net_info.default_gw = Serialization::decode_vstr(bufp, remainp);
    m_categories |= NETINFO;
  }
  else if (group == NET_GROUP) {
    net_stat.tcp_established = Serialization::decode_i32(bufp, remainp);
    net_stat.tcp_listen = Serialization::decode_i32(bufp, remainp);
    net_stat.tcp_time_wait = Serialization::decode_i32(bufp, remainp);
    net_stat.tcp_close_wait = Serialization::decode_i32(bufp, remainp);
    net_stat.tcp_idle = Serialization::decode_i32(bufp, remainp);
    net_stat.rx_rate = Serialization::decode_double(bufp, remainp);
    net_stat.tx_rate = Serialization::decode_double(bufp, remainp);
    m_categories |= NET;
  }
  else if (group == OSINFO_GROUP) {
    os_info.name = Serialization::decode_vstr(bufp, remainp);
    os_info.version = Serialization::decode_vstr(bufp, remainp);
    os_info.version_major = Serialization::decode_i16(bufp, remainp);
    os_info.version_minor = Serialization::decode_i16(bufp, remainp);
    os_info.version_micro = Serialization::decode_i16(bufp, remainp);
    os_info.arch = Serialization::decode_vstr(bufp, remainp);
    os_info.machine = Serialization::decode_vstr(bufp, remainp);
    os_info.description = Serialization::decode_vstr(bufp, remainp);
    os_info.patch_level = Serialization::decode_vstr(bufp, remainp);
    os_info.vendor = Serialization::decode_vstr(bufp, remainp);
    os_info.vendor_version = Serialization::decode_vstr(bufp, remainp);
    os_info.vendor_name = Serialization::decode_vstr(bufp, remainp);
    os_info.code_name = Serialization::decode_vstr(bufp, remainp);
    m_categories |= OSINFO;
  }
  else if (group == PROCINFO_GROUP) {
    proc_info.pid = Serialization::decode_i64(bufp, remainp);
    proc_info.user = Serialization::decode_vstr(bufp, remainp);
    proc_info.exe = Serialization::decode_vstr(bufp, remainp);
    proc_info.cwd = Serialization::decode_vstr(bufp, remainp);
    proc_info.root = Serialization::decode_vstr(bufp, remainp);
    size_t len = Serialization::decode_vi32(bufp, remainp);
    for (size_t i=0; i<len; i++)
      proc_info.args.push_back(Serialization::decode_vstr(bufp, remainp));
    m_categories |= PROCINFO;
  }
  else if (group == PROC_GROUP) {
    proc_stat.cpu_user = Serialization::decode_i64(bufp, remainp);
    proc_stat.cpu_sys = Serialization::decode_i64(bufp, remainp);
    proc_stat.cpu_total = Serialization::decode_i64(bufp, remainp);
    proc_stat.cpu_pct = Serialization::decode_double(bufp, remainp);
    proc_stat.vm_size = Serialization::decode_double(bufp, remainp);
    proc_stat.vm_resident = Serialization::decode_double(bufp, remainp);
    proc_stat.vm_share = Serialization::decode_double(bufp, remainp);
    proc_stat.minor_faults = Serialization::decode_i64(bufp, remainp);
    proc_stat.major_faults = Serialization::decode_i64(bufp, remainp);
    proc_stat.page_faults = Serialization::decode_i64(bufp, remainp);
    m_categories |= PROC;
  }
  else if (group == FS_GROUP) {
    size_t count = Serialization::decode_vi32(bufp, remainp);
    fs_stat.resize(count);
    for (size_t i=0; i<count; i++) {
      fs_stat[i].prefix = Serialization::decode_vstr(bufp, remainp);
      fs_stat[i].use_pct = Serialization::decode_double(bufp, remainp);
      fs_stat[i].total = Serialization::decode_i64(bufp, remainp);
      fs_stat[i].free = Serialization::decode_i64(bufp, remainp);
      fs_stat[i].used = Serialization::decode_i64(bufp, remainp);
      fs_stat[i].avail = Serialization::decode_i64(bufp, remainp);
      fs_stat[i].files = Serialization::decode_i64(bufp, remainp);
      fs_stat[i].free_files = Serialization::decode_i64(bufp, remainp);
    }
    m_categories |= FS;
  }
  else if (group == TERMINFO_GROUP) {
    term_info.term = Serialization::decode_vstr(bufp, remainp);
    term_info.num_lines = Serialization::decode_i32(bufp, remainp);
    term_info.num_cols = Serialization::decode_i32(bufp, remainp);
    m_categories |= TERMINFO;
  }
  else {
    HT_WARNF("Unrecognized StatsSystem group %d, skipping...", group);
    (*bufp) += len;
    (*remainp) -= len;
  }
}
