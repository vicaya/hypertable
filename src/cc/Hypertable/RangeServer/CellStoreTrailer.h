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

#ifndef HYPERTABLE_CELLSTORETRAILER_H
#define HYPERTABLE_CELLSTORETRAILER_H

#include <iostream>

extern "C" {
#include <stdint.h>
}

namespace Hypertable {

  /**
   * Abstract base class for cell store trailer.
   */
  class CellStoreTrailer {
  public:

    virtual ~CellStoreTrailer() { return; }

    /**
     * Clears the contents of this trailer;
     */
    virtual void clear() = 0;

    /**
     * Returns the serialized size of the trailer.
     *
     * @return serialized size of trailer
     */
    virtual size_t size() = 0;

    /**
     * Serializes this trailer to the given buffer;
     *
     * @param buf pointer to buffer to serialize trailer to
     */
    virtual void serialize(uint8_t *buf) = 0;

    /**
     * Deserializes the trailer from the given buffer
     * 
     * @param buf pointer to buffer containing serialized trailer
     */
    virtual void deserialize(uint8_t *buf) = 0;

    /**
     * Prints the trailer to the given ostream
     *
     * @param os ostream to print trailer to
     */
    virtual void display(std::ostream &os) = 0;

  };

  /**
   * ostream shift function for CellStoreTrailer objects
   */
  inline std::ostream &operator <<(std::ostream &os, CellStoreTrailer &trailer) {
    trailer.display(os);
    return os;
  }


}

#endif // HYPERTABLE_CELLSTORETRAILER_H
