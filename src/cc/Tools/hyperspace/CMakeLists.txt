#
# Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# 

set ( SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc" )
set ( DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Tools/hyperspace" )

# hyperspace - Hyperspace client program
add_executable (hyperspace hyperspace.cc CommandCreate.cc CommandMkdirs.cc CommandAttrSet.cc CommandAttrGet.cc CommandAttrDel.cc CommandExists.cc CommandDelete.cc)
target_link_libraries (hyperspace pthread ${BOOST_LIB} Hyperspace AsyncComm Common log4cpp)

# hsTest
add_executable (hsTest hsTest.cc)
target_link_libraries (hsTest pthread ${BOOST_LIB} Hyperspace AsyncComm Common log4cpp)

configure_file(${SRC_DIR}/Tools/hyperspace/hsTest.golden ${DST_DIR}/hsTest.golden)

install (TARGETS hyperspace RUNTIME DESTINATION bin)

#add_test (Hyperspace hsTest)
