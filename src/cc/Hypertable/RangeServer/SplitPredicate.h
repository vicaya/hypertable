/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_SPLIT_PREDICATE_H
#define HYPERTABLE_SPLIT_PREDICATE_H

namespace Hypertable {

  /**
   */
  class SplitPredicate {
  public:
    SplitPredicate() : m_split_pending(false) { }

    void load(const String &split_row, bool split_off_high) {
      m_split_row_str = split_row;
      m_split_row = m_split_row_str.c_str();
      m_split_off_high = split_off_high;
      m_split_pending = true;
    }

    bool split_off(const char *row) {
      int cmp = strcmp(row, m_split_row);
      return ((cmp <= 0 && !m_split_off_high) || (cmp > 0 && m_split_off_high));
    }

    void clear() { m_split_pending = false; }

    bool split_pending() { return m_split_pending; }

  private:
    String m_split_row_str;
    const char *m_split_row;
    bool m_split_off_high;
    bool m_split_pending;
  };

}

#endif // HYPERTABLE_SPLIT_PREDICATE_H
