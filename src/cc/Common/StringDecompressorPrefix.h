/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
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

#ifndef HYPERTABLE_STRINGDECOMPRESSORPREFIX_H
#define HYPERTABLE_STRINGDECOMPRESSORPREFIX_H

#include "ReferenceCount.h"
#include "String.h"

namespace Hypertable {

  class StringDecompressorPrefix : public ReferenceCount {
  public:
    StringDecompressorPrefix() : m_compressed_len(0) { }

    virtual void reset();
    virtual const uint8_t *add(const uint8_t *ptr);
    virtual size_t length();
    virtual size_t length_uncompressed();
    virtual void load(String &str);
  private:
    String m_last_string;
    size_t m_compressed_len;
  };
  typedef intrusive_ptr<StringDecompressorPrefix> StringDecompressorPrefixPtr;
}

#endif // HYPERTABLE_STRINGDECOMPRESSORPREFIX_H
