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
    RangeState() : state(STEADY), soft_limit(0), transfer_log(0),
		   split_point(0), old_start_row(0) { return; }

    void clear() {
      state = STEADY;
      timestamp = 0;
      //soft_limit = 0;  NOTE: this should not be cleared
      transfer_log = split_point = old_start_row = 0;
    }

    size_t encoded_length();
    void encode(uint8_t **bufp);
    void decode(const uint8_t **bufp, size_t *remainp);

    int state;
    int64_t timestamp;
    uint64_t soft_limit;
    const char *transfer_log;
    const char *split_point;
    const char *old_start_row;
  };

  std::ostream &operator<<(std::ostream &, const RangeState &);

  /**
   * Holds the storage the persistent state of a Range.
   */
  class RangeStateManaged : public RangeState {
  public:
    RangeStateManaged() { }
    RangeStateManaged(const RangeState &rs) {
      operator=(rs);
    }
    RangeStateManaged& operator=(const RangeState &rs) {
      state = rs.state;
      timestamp = rs.timestamp;
      soft_limit = rs.soft_limit;
      if (rs.transfer_log) {
        m_transfer_log = rs.transfer_log;
        transfer_log = m_transfer_log.c_str();
      }
      else
        transfer_log = 0;
      if (rs.split_point) {
        m_split_point = rs.split_point;
        split_point = m_split_point.c_str();
      }
      else
	split_point = 0;
      if (rs.old_start_row) {
	m_old_start_row = rs.old_start_row;
	old_start_row = m_old_start_row.c_str();
      }
      else
	old_start_row = 0;
      return *this;
    }
    
    void set_transfer_log(const String &tl) {
      m_transfer_log = tl;
      transfer_log = m_transfer_log.c_str();
    }

    void clear_transfer_log() {
      m_transfer_log = "";
      transfer_log = 0;
    }

    void set_split_point(const String &sp) {
      m_split_point = sp;
      split_point = m_split_point.c_str();
    }

    void clear_split_point() {
      m_split_point = "";
      split_point = 0;
    }

    void set_old_start_row(const String &osr) {
      m_old_start_row = osr;
      old_start_row = m_old_start_row.c_str();
    }

    void clear_old_start_row() {
      m_old_start_row = "";
      old_start_row = 0;
    }

  private:
    String m_transfer_log;
    String m_split_point;
    String m_old_start_row;
  };

}


#endif // HYPERTABLE_RANGESTATE_H
