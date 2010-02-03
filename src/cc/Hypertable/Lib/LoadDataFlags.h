/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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

#ifndef HYPERTABLE_LOADDATAFLAGS_H
#define HYPERTABLE_LOADDATAFLAGS_H

namespace Hypertable { namespace LoadDataFlags {

  enum {
    DUP_KEY_COLS       = 0x0001,
    NO_ESCAPE          = 0x0002,
    IGNORE_UNKNOWN_CFS = 0x0004,
    SINGLE_CELL_FORMAT = 0x0008
  };

  inline bool duplicate_key_columns(int flags) {
    return (flags & DUP_KEY_COLS) == DUP_KEY_COLS;
  }

  inline bool no_escape(int flags) {
    return (flags & NO_ESCAPE) == NO_ESCAPE;
  }

  inline bool ignore_unknown_cfs(int flags) {
    return (flags & IGNORE_UNKNOWN_CFS) == IGNORE_UNKNOWN_CFS;
  }

  inline bool single_cell_format(int flags) {
    return (flags & SINGLE_CELL_FORMAT) == SINGLE_CELL_FORMAT;
  }

} }


#endif // HYPERTABLE_LOADDATAFLAGS_H
