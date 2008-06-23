/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_RANGESTATE_H
#define HYPERTABLE_RANGESTATE_H

#include "Common/String.h"

namespace Hypertable {

  /**
   * Holds the persistent state of a Range.
   */
  class RangeState {
  public:
    enum StateType { STEADY, SPLIT_LOG_INSTALLED, SPLIT_SHRUNK };
    RangeState() : state(STEADY), soft_limit(0), transfer_log(0) { return; }
    RangeState(StateType st, uint64_t slimit, const char *tlog)
        : state(st), soft_limit(slimit), transfer_log(tlog) {}

    size_t encoded_length();
    void encode(uint8_t **bufp);
    void decode(const uint8_t **bufp, size_t *remainp);

    int state;
    uint64_t soft_limit;
    const char *transfer_log;
  };

  std::ostream &operator<<(std::ostream &, const RangeState &);

  /**
   * Holds the storage the persistent state of a Range.
   */
  class RangeStateManaged : public RangeState {
  public:
    RangeStateManaged(const RangeState &rs) {
      operator=(rs);
    }
    RangeStateManaged& operator=(const RangeState &rs) {
      state = rs.state;
      soft_limit = rs.soft_limit;
      if (rs.transfer_log) {
        m_transfer_log = rs.transfer_log;
        transfer_log = m_transfer_log.c_str();
      }
      else
        transfer_log = 0;
      return *this;
    }

    void set_transfer_log(const String &tl) {
      m_transfer_log = tl;
      transfer_log = m_transfer_log.c_str();
    }

  private:
    String m_transfer_log;
  };

}


#endif // HYPERTABLE_RANGESTATE_H
