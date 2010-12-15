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

#ifndef HYPERTABLE_STATSSYSTEM_H
#define HYPERTABLE_STATSSYSTEM_H

#include "Common/StatsSerializable.h"
#include "Common/SystemInfo.h"

namespace Hypertable {

  class StatsSystem : public StatsSerializable {
  public:

    enum Category {
      CPUINFO  = 0x0001,
      CPU      = 0x0002,
      LOADAVG  = 0x0004,
      MEMORY   = 0x0008,
      DISK     = 0x0010,
      SWAP     = 0x0020,
      NETINFO  = 0x0040,
      NET      = 0x0080,
      OSINFO   = 0x0100,
      PROCINFO = 0x0200,
      PROC     = 0x0400,
      FS       = 0x0800,
      TERMINFO = 0x1000
    };

    StatsSystem() : StatsSerializable(SYSTEM, 0), m_categories(0) { }
    StatsSystem(int32_t categories);
    StatsSystem(int32_t categories, std::vector<String> &dirs);
    void add_categories(int32_t categories);
    void add_categories(int32_t categories, std::vector<String> &dirs);
    void refresh();
    bool operator==(const StatsSystem &other) const;
    bool operator!=(const StatsSystem &other) const {
      return !(*this == other);
    }
    
    struct CpuInfo cpu_info;
    struct CpuStat cpu_stat;
    struct LoadAvgStat loadavg_stat;
    struct MemStat mem_stat;
    struct SwapStat swap_stat;
    struct NetInfo net_info;
    struct NetStat net_stat;
    struct OsInfo os_info;
    struct ProcInfo proc_info;
    struct ProcStat proc_stat;
    struct TermInfo term_info;
    std::vector<struct DiskStat> disk_stat;
    std::vector<struct FsStat> fs_stat;

  protected:
    virtual size_t encoded_length_group(int group) const;
    virtual void encode_group(int group, uint8_t **bufp) const;
    virtual void decode_group(int group, uint16_t len, const uint8_t **bufp, size_t *remainp);
    
  private:
    int32_t m_categories;
  };
  typedef intrusive_ptr<StatsSystem> StatsSystemPtr;

}


#endif // HYPERTABLE_STATSSYSTEM_H


