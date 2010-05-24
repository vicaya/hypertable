/**
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef HYPERTABLE_SERIALIZEDCELLSFLAG_H
#define HYPERTABLE_SERIALIZEDCELLSFLAG_H

namespace Hypertable {
  namespace SerializedCellsFlag {
    enum {
      EOB            = 0x01,
      EOS            = 0x02,
      FLUSH          = 0x04,
      REV_IS_TS      = 0x10,
      AUTO_TIMESTAMP = 0x20,
      HAVE_TIMESTAMP = 0x40,
      HAVE_REVISION  = 0x80
    };
  }
}

#endif // HYPERTABLE_SERIALIZEDCELLSFLAG_H
