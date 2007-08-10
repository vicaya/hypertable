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

#ifndef HYPERTABLE_BYTEORDER_H
#define HYPERTABLE_BYTEORDER_H

#define ByteOrderSwapInt64(x) ((((uint64_t)(x) & 0xff00000000000000ULL) >> 56) | \
                               (((uint64_t)(x) & 0x00ff000000000000ULL) >> 40) | \
                               (((uint64_t)(x) & 0x0000ff0000000000ULL) >> 24) | \
                               (((uint64_t)(x) & 0x000000ff00000000ULL) >>  8) | \
                               (((uint64_t)(x) & 0x00000000ff000000ULL) <<  8) | \
                               (((uint64_t)(x) & 0x0000000000ff0000ULL) << 24) | \
                               (((uint64_t)(x) & 0x000000000000ff00ULL) << 40) | \
                               (((uint64_t)(x) & 0x00000000000000ffULL) << 56))

#endif // HYPERTABLE_BYTEORDER_H

