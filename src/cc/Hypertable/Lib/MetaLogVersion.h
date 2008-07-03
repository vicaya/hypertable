/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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

#ifndef HYPERTABLE_RANGE_SERVER_METALOG_VERSION_H
#define HYPERTABLE_RANGE_SERVER_METALOG_VERSION_H

namespace Hypertable {

// sizes
enum {
  ML_ENTRY_HEADER_SIZE = 4 + 1 + 8 + 4,
  RSML_HEADER_SIZE = 5 + 2,
  MML_HEADER_SIZE = 3 + 2
};

// range server constants
extern const uint16_t RSML_VERSION;
extern const char *RSML_PREFIX;

// master constants
extern const uint16_t MML_VERSION;
extern const char *MML_PREFIX;

} // namespace Hypertable

#endif // HYPERTABLE_RANGE_SERVER_METALOG_VERSION_H


