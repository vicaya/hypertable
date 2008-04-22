/**
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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

#ifndef HYPERTABLE_FIXED_STREAM_H
#define HYPERTABLE_FIXED_STREAM_H

#include "String.h"
#include <streambuf>
#include <istream>
#include <ostream>

namespace Hypertable {

/**
 * A simple streambuf with fixed size buffer
 * Convenient for limitting size of output; faster than ostringstream
 * and friends, which require heap allocations
 */
class FixedStreamBuf : public std::streambuf {
public:
  FixedStreamBuf(char *buf, size_t len) { setp(buf, buf + len); }
  FixedStreamBuf(const char *buf, const char *next, const char *end) {
    setg((char *)buf, (char *)next, (char *)end);
  }
  String str() { return String(pbase(), pptr()); }
};

/**
 * Output stream using a fixed buffer
 */
class FixedOstream : private virtual FixedStreamBuf, public std::ostream {
public:
  typedef FixedStreamBuf StreamBuf;

  FixedOstream(char *buf, size_t len) : FixedStreamBuf(buf, len),
               std::ostream(static_cast<StreamBuf *>(this)) {}

  char *output() { return StreamBuf::pbase(); }
  char *output_ptr() { return StreamBuf::pptr(); }
  char *output_end() { return StreamBuf::epptr(); }
  String str() { return StreamBuf::str(); }
};

/**
 * Input stream using a fixed buffer
 */
class FixedIstream : private virtual FixedStreamBuf, public std::istream {
public:
  typedef FixedStreamBuf StreamBuf;

  FixedIstream(const char *buf, const char *end) :
               FixedStreamBuf(buf, buf, end),
               std::istream(static_cast<StreamBuf *>(this)) {}

  char *input() { return StreamBuf::eback(); }
  char *input_ptr() { return StreamBuf::gptr(); }
  char *input_end() { return StreamBuf::egptr(); }
};

} // namespace Hypertable

#endif // HYPERTABLE_FIXED_STREAM_H
