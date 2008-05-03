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
    enum { STEADY, SPLIT_LOG_INSTALLED, SPLIT_SHRUNK };
    RangeState() : state(STEADY), soft_limit(0), transfer_log(0) { return; }
    size_t encoded_length();
    void encode(uint8_t **bufPtr);
    bool decode(uint8_t **bufPtr, size_t *remainingPtr);
    int state;
    uint64_t soft_limit;
    const char *transfer_log;
  };


  /**
   * Complete copy of the persistent state of a Range.
   */
  class RangeStateCopy : public RangeState {
  public:
    RangeStateCopy(RangeState &rs) {
      state = rs.state;
      soft_limit = rs.soft_limit;
      if (rs.transfer_log) {
	transfer_log_str = rs.transfer_log;
	transfer_log = transfer_log_str.c_str();
      }
      else
	transfer_log = 0;
    }
    RangeStateCopy(RangeStateCopy &rsc) {
      state = rsc.state;
      soft_limit = rsc.soft_limit;
      if (rsc.transfer_log) {
	transfer_log_str = rsc.transfer_log;
	transfer_log = transfer_log_str.c_str();
      }
      else
	transfer_log = 0;
    }
    void set_transfer_log(const String &tl) {
      transfer_log_str = tl;
      transfer_log = transfer_log_str.c_str();
    }
    
  private:
    String transfer_log_str;
  };

}


#endif // HYPERTABLE_RANGESTATE_H
