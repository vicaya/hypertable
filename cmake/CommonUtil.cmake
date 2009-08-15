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

# This is used by many thrift targets to guard against make clean
macro(HT_GLOB var)
  file(GLOB_RECURSE ${var} ${ARGN})
  if (NOT ${var}) # make clean would remove generated files
    # add a dummy target here
    set(${var} "${var}_DUMMY")
  endif ()
  #message(STATUS "${var}: ${${var}}")
endmacro()

if (PACKAGE_THRIFTBROKER)
  set(HT_COMPONENT_INSTALL true)
  message(STATUS "Prepare for ThriftBroker only installation")
endif ()
