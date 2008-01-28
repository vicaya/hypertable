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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef HYPERTABLE_COMPAT_C_H
#define HYPERTABLE_COMPAT_C_H

/** Portability macros for C code. */

/* Name mangling */
#ifdef __cplusplus
#  define HT_EXTERN_C  extern "C"
#else
#  define HT_EXTERN_C  extern
#endif

/* Calling convention */
#ifdef _MSC_VER
#  define HT_CDECL      __cdecl
#else
#  define HT_CDECL
#endif

#define HT_PUBLIC(ret_type)     ret_type HT_CDECL
#define HT_EXTERN(ret_type)     HT_EXTERN_C HT_PUBLIC(ret_type)

#ifdef __GNUC__
#  define HT_NORETURN __attribute__((__noreturn__))
#  define HT_FORMAT(x) __attribute__((format x))
#else
#  define HT_NORETURN
#  define HT_FORMAT(x)
#  ifndef __attribute__
#    define __attribute__(x)
#  endif
#endif

#endif /* HYPERTABLE_COMPAT_C_H */
