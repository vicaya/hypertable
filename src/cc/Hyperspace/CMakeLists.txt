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

set ( SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Hyperspace" )
set ( DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Hyperspace" )

add_library (Hyperspace HyperspaceClient.cc HyperspaceProtocol.cc)

# hsClient - Hyperspace client program
add_executable (hsClient tests/hyperspace.cc tests/CommandCreate.cc tests/CommandMkdirs.cc tests/CommandAttrSet.cc tests/CommandAttrGet.cc tests/CommandAttrDel.cc tests/CommandExists.cc tests/CommandDelete.cc)
target_link_libraries (hsClient pthread boost_thread Hyperspace AsyncComm Common log4cpp)

# hsTest
add_executable (hsTest tests/hsTest.cc)
target_link_libraries (hsTest pthread boost_thread Hyperspace AsyncComm Common log4cpp)

#add_custom_command (SOURCE ${SRC_DIR}/tests
#                    COMMAND ln ARGS -s ${SRC_DIR}/tests ${DST_DIR}/tests
#                    OUTPUT ${DST_DIR}/tests)

configure_file(${SRC_DIR}/tests/hsTest.golden ${DST_DIR}/hsTest.golden)

install (TARGETS hsClient RUNTIME DESTINATION bin)

add_test (Hyperspace hsTest)
