#!/bin/bash
#
# Copyright (C) 2009  Luke Lu (llu@hypertable.org)
#
# This file is part of Hypertable.
#
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or any later version.
#
# Hypertable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Hypertable. If not, see <http://www.gnu.org/licenses/>
#

# Figure out soname of a shared lib. Handles platforms with GNU binutils, e.g.,
# Linux etc; Solaris with elfdump and Mac OS X with otool
#
lib=$1

if o=`(objdump -p $lib | grep SONAME) 2>/dev/null` ||
   o=`(elfdump -d $lib | grep SONAME) 2>/dev/null`
then
  echo $o | sed 's/.* \([^ ][^ ]*\)/\1/'
elif o=`otool -D $l 2>/dev/null`; then
  echo $o | sed 's/.*\/\([^\/][^\/]*\)/\1/'
fi
