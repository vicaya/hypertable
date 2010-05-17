/** -*- c++ -*-
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
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
#include "Common/StringExt.h"

#include <algorithm>
#include "ServerStats.h"
#include "SystemInfo.h"

namespace Hypertable {

ServerStatsBundle& ServerStatsBundle::operator += (const ServerStatsBundle& other) {
  disk_available     += other.disk_available;
  disk_used          += other.disk_used;
  disk_read_KBps     += other.disk_read_KBps;
  disk_write_KBps    += other.disk_write_KBps;
  disk_read_rate     += other.disk_read_rate;
  disk_write_rate    += other.disk_write_rate;
  mem_total          += other.mem_total;
  mem_used           += other.mem_used;
  vm_size            += other.vm_size;
  vm_resident        += other.vm_resident;
  net_recv_KBps      += other.net_recv_KBps;
  net_send_KBps      += other.net_send_KBps;
  loadavg_0          += other.loadavg_0;
  loadavg_1          += other.loadavg_1;
  loadavg_2          += other.loadavg_2;
  cpu_pct            += other.cpu_pct;
  num_cores          = std::max(num_cores, other.num_cores);
  clock_mhz          += other.clock_mhz;
  return *this;
}

void ServerStatsBundle::dump_str(String &out) const {
  out += (String)"disk_available KB=" + disk_available;
  out += (String)", disk_used KB=" + disk_used;
  out += (String)", disk_read_KBps=" + disk_read_KBps;
  out += (String)", disk_write_KBps=" + disk_write_KBps;
  out += (String)", disk_read_rate=" + disk_read_rate;
  out += (String)", disk_write_rate=" + disk_write_rate;
  out += (String)", mem_total KB=" + mem_total;
  out += (String)", mem_used KB=" + mem_used;
  out += (String)", vm_size KB=" + vm_size;
  out += (String)", vm_resident KB=" + vm_resident;
  out += (String)", net_recv_KBps=" + net_recv_KBps;
  out += (String)", net_send_KBps=" + net_send_KBps;
  out += format(", loadavg_0=%.2f", ((float)loadavg_0)/100);
  out += format(", loadavg_1=%.2f", ((float)loadavg_1)/100);
  out += format(", loadavg_2=%.2f", ((float)loadavg_2)/100);
  out += format(", cpu_pct=%.2f", ((float)cpu_pct)/100);
  out += (String)", num_cores=" + num_cores;
  out += (String)", clock_mhz=" + clock_mhz;
}

const ServerStatsBundle ServerStatsBundle::operator+(const ServerStatsBundle &other) const {
  ServerStatsBundle result = *this;
  result += other;
  return result;
}

ServerStatsBundle& ServerStatsBundle::operator/=(uint32_t count) {

  HT_ASSERT(count > 0);
  disk_available     = (int64_t)(disk_available/count);
  disk_used          = (int64_t)(disk_used/count);
  disk_read_KBps     = (int32_t)(disk_read_KBps/count);
  disk_write_KBps    = (int32_t)(disk_write_KBps/count);
  disk_read_rate     = (int32_t)(disk_read_rate/count);
  disk_write_rate    = (int32_t)(disk_write_rate/count);
  mem_total          = (int32_t)(mem_total/count);
  mem_used           = (int32_t)(mem_used/count);
  vm_size            = (int32_t)(vm_size/count);
  vm_resident        = (int32_t)(vm_resident/count);
  net_recv_KBps      = (int32_t)(net_recv_KBps/count);
  net_send_KBps      = (int32_t)(net_send_KBps/count);
  loadavg_0          = (int16_t)(loadavg_0/count);
  loadavg_1          = (int16_t)(loadavg_1/count);
  loadavg_2          = (int16_t)(loadavg_2/count);
  cpu_pct            = (uint8_t)(cpu_pct/count);
  num_cores          = (uint8_t)(num_cores);
  clock_mhz          = (int32_t)(clock_mhz/count);
  return *this;

}

const ServerStatsBundle ServerStatsBundle::operator/(uint32_t count) const {
  HT_ASSERT(count > 0);
  ServerStatsBundle result = *this;
  result /= count;
  return result;
}

void ServerStats::recompute() {
  // disk stats
  m_stats.disk_available = (int32_t)(System::fs_stat().free * 1000);
  m_stats.disk_used = (int32_t)(System::fs_stat().used * 1000);
  m_stats.disk_read_KBps = (int32_t)(System::disk_stat().read_rate);
  m_stats.disk_write_KBps = (int32_t)(System::disk_stat().write_rate);
  m_stats.disk_read_rate = (int32_t)(System::disk_stat().reads_rate);
  m_stats.disk_write_rate = (int32_t)(System::disk_stat().writes_rate);

  // mem stats
  m_stats.mem_total = (int32_t)(System::mem_stat().total * 1000) ;
  m_stats.mem_used = (int32_t)(System::mem_stat().used * 1000);
  m_stats.vm_size = (int32_t)(System::proc_stat().vm_size * 1000);
  m_stats.vm_resident = (int32_t)(System::proc_stat().vm_resident * 1000);

  // Proc stats
  m_stats.cpu_pct = (int16_t)(System::proc_stat().cpu_pct*100);

  // net stats
  m_stats.net_recv_KBps = (int32_t)(System::net_stat().rx_rate);
  m_stats.net_send_KBps = (int32_t)(System::net_stat().tx_rate);

  // loadavg stats
  m_stats.loadavg_0 = (int16_t)(System::loadavg_stat().loadavg[0]*100);
  m_stats.loadavg_1 = (int16_t)(System::loadavg_stat().loadavg[1]*100);
  m_stats.loadavg_2 = (int16_t)(System::loadavg_stat().loadavg[2]*100);

  // cpu info
  m_stats.num_cores = (uint8_t)(System::cpu_info().total_cores);
  m_stats.clock_mhz = (int32_t)(System::cpu_info().mhz);
}

} // namespace Hypertable
