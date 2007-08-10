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
#ifndef HYPERTABLE_STRINGEXT_H
#define HYPERTABLE_STRINGEXT_H

#include <ext/hash_map>
#include <stdexcept>
#include <string>

extern "C" {
#include <stdio.h>
}

namespace __gnu_cxx {
  template<> struct hash< std::string >  {
    size_t operator()( const std::string& x ) const {
      return hash< const char* >()( x.c_str() );
    }
  };
}

inline std::string operator+( const std::string& s1, short sval ) {
  char cbuf[8];
  sprintf(cbuf, "%d", sval);
  return s1 + cbuf;
}

inline std::string operator+( const std::string& s1, int ival ) {
  char cbuf[16];
  sprintf(cbuf, "%d", ival);
  return s1 + cbuf;
}



#endif // HYPERTABLE_STRINGEXT_H
