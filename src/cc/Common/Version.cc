/**
 * Copyright (C) 2007 Luke Lu (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include "Common/Version.h"

namespace Hypertable {

const int version_major = HT_VERSION_MAJOR;
const int version_minor = HT_VERSION_MINOR;
const int version_micro = HT_VERSION_MICRO;
const int version_patch = HT_VERSION_PATCH;
const std::string version_misc_suffix = HT_VERSION_MISC_SUFFIX;
const char *version() {
  return HT_VERSION;
}

} // namespace Hypertable
