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

set (AsyncComm_SRCS
DispatchHandlerSynchronizer.cc
Comm.cc
ConnectionManager.cc
Event.cc
IOHandler.cc
IOHandlerAccept.cc
IOHandlerData.cc
IOHandlerDatagram.cc
HeaderBuilder.cc
Header.cc
Protocol.cc
Reactor.cc
ReactorFactory.cc
ReactorRunner.cc
RequestCache.cc
ResponseCallback.cc
)

add_library (AsyncComm ${AsyncComm_SRCS})

# sampleClient
add_executable (sampleClient sampleClient.cc)
target_link_libraries (sampleClient pthread ${BOOST_LIB} AsyncComm Common log4cpp)

# testServer
add_executable (testServer testServer.cc)
target_link_libraries (testServer pthread ${BOOST_LIB} AsyncComm Common log4cpp)

# commTest
add_executable (commTest tests/commTest.cc tests/CommTestThreadFunction.cc)
target_link_libraries (commTest pthread ${BOOST_LIB} AsyncComm Common log4cpp expat)

add_test (AsyncComm commTest)

install (TARGETS AsyncComm
         RUNTIME DESTINATION bin
         LIBRARY DESTINATION lib
         ARCHIVE DESTINATION lib/static)