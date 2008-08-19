/** -*- c++ -*-
 * Copyright (C) 2008 Donald <donaldliew@gmail.com>
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

#ifndef HYPERTABLE_STAT_H
#define HYPERTABLE_STAT_H

#include "Hypertable/Lib/Types.h"

namespace Hypertable {

  /** Statistics of a Range */
  class RangeStat {
  public:
    RangeStat() { return; }
    RangeStat(const uint8_t **bufp, size_t *remainp) {
	    decode(bufp, remainp);
    }

    TableIdentifierManaged table_identifier;
    RangeSpecManaged range_spec;

    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);

    uint64_t added_inserts;
    uint64_t added_deletes[3];
    uint64_t cached_cells;
    uint64_t collided_cells;

    uint64_t disk_usage;
    uint64_t memory_usage;
  };

  /** Statistics of a RangeServer */
  class RangeServerStat {
  public:
    RangeServerStat() { return; }
    RangeServerStat(const uint8_t **bufp, size_t *remainp) { decode(bufp, remainp); }

    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);

    std::vector<RangeStat> range_stats;
  };


  std::ostream &operator<<(std::ostream &os, const RangeStat &);

  std::ostream &operator<<(std::ostream &os, const RangeServerStat &);


} // namespace Hypertable


#endif // HYPERTABLE_REQUEST_H
