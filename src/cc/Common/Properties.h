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


#ifndef HYPERTABLE_PROPERTIES_H
#define HYPERTABLE_PROPERTIES_H

#include <ext/hash_map>

#include "StringExt.h"

namespace hypertable {

  class Properties {

  public:

    Properties() { return; }

    Properties(std::string fname) {
      load(fname);
    }

    void load(const char *fname) throw(std::invalid_argument);

    void load(std::string &fname) { load(fname.c_str());  }

    const char *getProperty(const char *str);

    const char *getProperty(const char *str, const char *defaultValue);

    int getPropertyInt(const char *str, int defaultValue);

    int64_t getPropertyInt64(const char *str, int64_t defaultValue);

    bool containsKey(const char *str) {
      PropertyMapT::iterator iter = mMap.find(str);
      return (iter == mMap.end()) ? false : true;
    }

  private:

    typedef __gnu_cxx::hash_map<std::string, const char *> PropertyMapT;

    PropertyMapT  mMap;
  };

}

#endif // HYPERTABLE_PROPERTIES_H
