/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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

#ifndef HYPERTABLE_KEYDECOMPRESSORPREFIX_H
#define HYPERTABLE_KEYDECOMPRESSORPREFIX_H

#include "Common/DynamicBuffer.h"

#include "KeyDecompressor.h"

namespace Hypertable {

  class KeyDecompressorPrefix : public KeyDecompressor {
  public:
    virtual void reset();
    virtual const uint8_t *add(const uint8_t *ptr);
    virtual bool less_than(SerializedKey serialized_key);
    virtual void load(Key &key);
  private:
    SerializedKey m_serialized_key;
    DynamicBuffer m_bufs[2];
    const uint8_t *m_current_base;
    bool m_first;
  };
  typedef intrusive_ptr<KeyDecompressorPrefix> KeyDecompressorPrefixPtr;

}

#endif // HYPERTABLE_KEYDECOMPRESSORPREFIX_H
