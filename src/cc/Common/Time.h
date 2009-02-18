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

#ifndef HYPERTABLE_HIRES_TIME_H
#define HYPERTABLE_HIRES_TIME_H

#include <iosfwd>
#include <boost/thread/xtime.hpp>

namespace Hypertable {

  bool xtime_add_millis(boost::xtime &xt, uint32_t millis);
  bool xtime_sub_millis(boost::xtime &xt, uint32_t millis);
  uint64_t xtime_diff_millis(boost::xtime &early, boost::xtime &late);

  using boost::TIME_UTC;

  /** High-Resolution Time
   */
  struct HiResTime : public boost::xtime {
    HiResTime(int type = TIME_UTC) { reset(type); }

    /** get the current hi-res time
     *  return true on success; otherwise false
     */
    bool reset(int type = TIME_UTC) {
      return boost::xtime_get(this, type) != 0;
    }

    /** compare with other HiResTime
     *  <: -1; ==: 0; >: 1
     */
    int cmp(const HiResTime &y) const {
      return boost::xtime_cmp(*this, y);
    }

    bool operator<(const HiResTime &y) const { return cmp(y) < 0; }
    bool operator==(const HiResTime &y) const { return cmp(y) == 0; }

    HiResTime &operator+=(uint32_t ms) {
      xtime_add_millis(*this, ms);
      return *this;
    }

    HiResTime &operator-=(uint32_t ms) {
      xtime_sub_millis(*this, ms);
      return *this;
    }
  };

  uint64_t get_ts64();
  std::ostream &hires_ts(std::ostream &);
  std::ostream &hires_ts_date(std::ostream &);

} // namespace Hypertable

#endif // HYPERTABLE_HIRES_TIME_H
