/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
#ifndef HYPERTABLE_BLOCKINFLATERZLIB_H
#define HYPERTABLE_BLOCKINFLATERZLIB_H

extern "C" {
#include <zlib.h>
}

#include "BlockInflater.h"

using namespace hypertable;

namespace hypertable {

  class BlockInflaterZlib : public BlockInflater {
  public:
    BlockInflaterZlib();
    virtual ~BlockInflaterZlib();
    virtual bool inflate(uint8_t *zbuf, uint32_t zlen, const char magic[12], hypertable::DynamicBuffer &outbuf);

  private:
    z_stream  mStream;
  };
}

#endif // HYPERTABLE_BLOCKINFLATERZLIB_H
