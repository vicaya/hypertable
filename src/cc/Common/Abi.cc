/** -*- C++ -*-
 * Copyright (C) 2009  Luke Lu (Zvents, Inc.)
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

#include "Common/Compat.h"
#include "Common/Abi.h"

#ifdef __GXX_ABI_VERSION
#include <cxxabi.h>
#endif

namespace Hypertable {

String demangle(const String &mangled) {
#ifdef __GXX_ABI_VERSION
  char debuf[1000];
  size_t len = sizeof(debuf);
  int ret;
  char *demangled = abi::__cxa_demangle(mangled.c_str(), debuf, &len, &ret);

  if (demangled)
    return demangled;

#endif
  return mangled;
}

} // namespace Hypertable
