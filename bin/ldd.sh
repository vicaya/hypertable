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

# Do ldd on several platforms
case `uname -s` in
  Darwin)     ldd='otool -L';;
  *)          ldd=ldd;;
esac
case $1 in
  /*)         file=$1;;
  lib/*)      file=$HYPERTABLE_HOME/lib/$1;;
  *)          file=$HYPERTABLE_HOME/bin/$1;;
esac

exec $ldd "$file"
