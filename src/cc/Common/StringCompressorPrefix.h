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

#ifndef HYPERTABLE_STRINGCOMPRESSORPREFIX_H
#define HYPERTABLE_STRINGCOMPRESSORPREFIX_H

#include "ReferenceCount.h"
#include "DynamicBuffer.h"
#include "String.h"

namespace Hypertable {

  class StringCompressorPrefix: public ReferenceCount {
  public:
    virtual void reset();
    virtual void add(const char *);
    virtual size_t length();
    virtual size_t length_uncompressed();
    virtual void write(uint8_t *buf);
    virtual void write_uncompressed(uint8_t *buf);
  private:
    size_t get_prefix_length(const char *, size_t len);

    size_t m_compressed_len;
    size_t m_uncompressed_len;
    String m_last_string;
    DynamicBuffer m_compressed_string;
  };
  typedef intrusive_ptr<StringCompressorPrefix> StringCompressorPrefixPtr;

}

#endif // HYPERTABLE_STRINGCOMPRESSORPREFIX_H
