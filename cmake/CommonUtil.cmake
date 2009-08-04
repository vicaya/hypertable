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

# This is a workaround for install() which always preserves symlinks
macro(HT_INSTALL_COPY dest)
  foreach(fpath ${ARGN})
    if (NOT ${fpath} MATCHES "NOTFOUND$")
      #message(STATUS "install copy: ${fpath}")
      get_filename_component(fname ${fpath} NAME)
      configure_file(${fpath} "${dest}/${fname}" COPYONLY)
      install(FILES "${CMAKE_BINARY_DIR}/${dest}/${fname}" DESTINATION ${dest})
    endif ()
  endforeach()
endmacro()

if (PACKAGE_THRIFTBROKER)
  set(HT_COMPONENT_INSTALL true)
endif ()
