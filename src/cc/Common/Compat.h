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

#ifndef HYPERTABLE_COMPAT_H
#define HYPERTABLE_COMPAT_H

// Include this before other includes .cc files

// The same stuff for C code
#include "compat-c.h"

#include <cstddef> // for std::size_t and std::ptrdiff_t

// C++ specific stuff
#ifndef BOOST_SPIRIT_THREADSAFE
#  define BOOST_SPIRIT_THREADSAFE
#endif

#define BOOST_IOSTREAMS_USE_DEPRECATED

#include "Sweetener.h"

#define HT_UNUSED(x) static_cast<void>(x)

#endif // HYPERTABLE_COMPAT_H
