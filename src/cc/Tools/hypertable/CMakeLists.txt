#
# Copyright (C) 2008 Doug Judd (Zvents, Inc.)
# 
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# 

set ( SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Tools/hypertable" )
set ( DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Tools/hypertable" )

set (hypertable_SRCS
hypertable.cc
)

# hypertable - command interpreter
add_executable (hypertable ${hypertable_SRCS})
target_link_libraries (hypertable pthread ${BOOST_LIB} Hypertable log4cpp expat readline curses ${Tcmalloc_LIBRARIES})

# hypertable_test
add_executable (hypertable_test test/hypertable_test.cc ${TEST_DEPENDENCIES} )
target_link_libraries (hypertable_test pthread ${BOOST_LIB} HyperCommon log4cpp ${Tcmalloc_LIBRARIES})

install (TARGETS hypertable RUNTIME DESTINATION bin)

configure_file(${HYPERTABLE_SOURCE_DIR}/conf/hypertable.cfg  ${DST_DIR}/hypertable.cfg)
configure_file(${SRC_DIR}/test/hypertable_test.golden ${DST_DIR}/hypertable_test.golden)
configure_file(${SRC_DIR}/test/hypertable_test.hql ${DST_DIR}/hypertable_test.hql)

add_test (hypertable hypertable_test)

