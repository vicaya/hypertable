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

set (dfstool_SRCS
CommandCopyFromLocal.cc
CommandCopyToLocal.cc
CommandLength.cc
CommandMkdirs.cc
CommandRemove.cc
CommandShutdown.cc
)

# dfstool - command interpreter
add_executable (dfstool dfstool.cc ${dfstool_SRCS})
target_link_libraries (dfstool pthread ${BOOST_LIB} DfsBroker AsyncComm Hyperspace Common log4cpp expat readline curses)

# dfsTest
add_executable (dfsTest dfsTest.cc dfsTestThreadFunction.cc ${dfstool_SRCS})
target_link_libraries (dfsTest pthread ${BOOST_LIB} DfsBroker AsyncComm Common log4cpp)

add_test (DfsBroker dfsTest)

install (TARGETS dfstool RUNTIME DESTINATION bin)

