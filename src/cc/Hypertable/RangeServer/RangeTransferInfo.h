/**
 * Copyright (C) 2010 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_RANGE_TRANSFER_INFO_H
#define HYPERTABLE_RANGE_TRANSFER_INFO_H

namespace Hypertable {

  /**
   */
  class RangeTransferInfo {
  public:
    RangeTransferInfo() { }

    void set_split(const String &split_row, bool split_off_high) {
      m_split_row = split_row;
      m_split_off_high = split_off_high;
    }

    bool transferring(const char *row) {
      if (m_split_row.length() == 0)
        return true;
      int cmp = strcmp(row, m_split_row.c_str());
      return ((cmp <= 0 && !m_split_off_high) || (cmp > 0 && m_split_off_high));
    }

    void clear() { m_split_row = ""; }

  private:
    String m_split_row;
    bool m_split_off_high;
  };


}

#endif // HYPERTABLE_RANGE_TRANSFER_INFO_H
