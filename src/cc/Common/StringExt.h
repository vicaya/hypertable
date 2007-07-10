/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
