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

set ( SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Tools/hyperspace" )
set ( DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Tools/hyperspace" )

set ( hyperspace_SRCS
CommandMkdir.cc
CommandDelete.cc
CommandOpen.cc
CommandCreate.cc
CommandClose.cc
CommandAttrSet.cc
CommandAttrGet.cc
CommandAttrDel.cc
CommandExists.cc
CommandReaddir.cc
CommandLock.cc
CommandTryLock.cc
CommandRelease.cc
CommandGetSequencer.cc
hyperspace.cc
Global.cc
Util.cc
)

# hyperspace - Hyperspace client program
add_executable (hyperspace ${hyperspace_SRCS} )
target_link_libraries (hyperspace pthread ${BOOST_LIB} Hyperspace HyperComm HyperCommon log4cpp readline curses ${Tcmalloc_LIBRARIES})

# hyperspaceTest
add_executable (hyperspaceTest test/hyperspaceTest.cc)
target_link_libraries (hyperspaceTest pthread ${BOOST_LIB} HyperComm HyperCommon log4cpp ${Tcmalloc_LIBRARIES})

configure_file(${SRC_DIR}/test/hyperspaceTest.cfg ${DST_DIR}/hyperspaceTest.cfg)
configure_file(${SRC_DIR}/test/client1.golden ${DST_DIR}/client1.golden COPYONLY)
configure_file(${SRC_DIR}/test/client2.golden ${DST_DIR}/client2.golden COPYONLY)
configure_file(${SRC_DIR}/test/client3.golden ${DST_DIR}/client3.golden COPYONLY)

install (TARGETS hyperspace RUNTIME DESTINATION bin)

add_test (Hyperspace hyperspaceTest)
