#
# Copyright (C) 2008 Doug Judd (Zvents, Inc.)
# 
# This file is part of Hypertable.
# 
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2 of
# the License.
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

set ( Hyperspace_SRCS
ClientKeepaliveHandler.cc
ClientConnectionHandler.cc
DirEntry.cc
HandleCallback.cc
Protocol.cc
Session.cc
)

# Hyperspace library
add_library (Hyperspace ${Hyperspace_SRCS})

set ( Master_SRCS
Event.cc
Master.cc
RequestHandlerMkdir.cc
RequestHandlerDelete.cc
RequestHandlerOpen.cc
RequestHandlerClose.cc
RequestHandlerAttrSet.cc
RequestHandlerAttrGet.cc
RequestHandlerAttrDel.cc
RequestHandlerExists.cc
RequestHandlerReaddir.cc
RequestHandlerLock.cc
RequestHandlerRelease.cc
RequestHandlerStatus.cc
ResponseCallbackOpen.cc
ResponseCallbackExists.cc
ResponseCallbackAttrGet.cc
ResponseCallbackLock.cc
ResponseCallbackReaddir.cc
ServerConnectionHandler.cc
ServerKeepaliveHandler.cc
main.cc
)

# Hyperspace.Master
add_executable (Hyperspace.Master ${Master_SRCS})
target_link_libraries (Hyperspace.Master pthread ${BOOST_LIB} Hyperspace HyperDfsBroker HyperComm HyperCommon log4cpp)

file (GLOB HEADERS *.h)

install (FILES ${HEADERS} DESTINATION include/hypertable-${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_MICRO}/Hyperspace)

install (TARGETS Hyperspace.Master RUNTIME DESTINATION bin)

install (TARGETS Hyperspace
         RUNTIME DESTINATION bin
         LIBRARY DESTINATION lib
         ARCHIVE DESTINATION lib)
