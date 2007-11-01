#
# Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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

set ( SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Tools/dfsclient" )
set ( DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Tools/dfsclient" )


set (dfsclient_SRCS
CommandCopyFromLocal.cc
CommandCopyToLocal.cc
CommandLength.cc
CommandMkdirs.cc
CommandRemove.cc
CommandRmdir.cc
CommandShutdown.cc
)

# dfsclient - command interpreter
add_executable (dfsclient dfsclient.cc ${dfsclient_SRCS})
target_link_libraries (dfsclient pthread ${BOOST_LIB} DfsBroker AsyncComm Hyperspace Common log4cpp expat readline curses)

# dfsTest
add_executable (dfsTest dfsTest.cc dfsTestThreadFunction.cc ${dfsclient_SRCS})
target_link_libraries (dfsTest pthread ${BOOST_LIB} DfsBroker AsyncComm Common log4cpp)

configure_file(${SRC_DIR}/dfsTest.golden ${DST_DIR}/dfsTest.golden COPYONLY)

add_test (DfsBroker dfsTest)

install (TARGETS dfsclient RUNTIME DESTINATION bin)

