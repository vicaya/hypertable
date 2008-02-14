/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_CELL_H
#define HYPERTABLE_CELL_H

namespace Hypertable {

  /** Encapsulates decomposed key and value */
  typedef struct {
    const char *row_key;
    const char *column_family;
    const char *column_qualifier;
    uint64_t timestamp;
    const uint8_t *value;
    uint64_t value_len;
    uint8_t flag;
  } CellT;

}

#endif // HYPERTABLE_CELL_H
