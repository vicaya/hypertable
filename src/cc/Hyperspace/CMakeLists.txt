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

set ( Hyperspace_SRCS
ClientKeepaliveHandler.cc
ClientConnectionHandler.cc
HyperspaceClient.cc
HyperspaceProtocol.cc
Protocol.cc
Session.cc
)

# Hyperspace library
add_library (Hyperspace ${Hyperspace_SRCS})

set ( Master_SRCS
Master.cc
RequestHandlerMkdir.cc
RequestHandlerDelete.cc
RequestHandlerOpen.cc
RequestHandlerClose.cc
RequestHandlerAttrSet.cc
RequestHandlerAttrDel.cc
RequestHandlerExists.cc
ResponseCallbackOpen.cc
ResponseCallbackExists.cc
ServerConnectionHandler.cc
ServerKeepaliveHandler.cc
main.cc
)

# Hyperspace.Master
add_executable (Hyperspace.Master ${Master_SRCS})
target_link_libraries (Hyperspace.Master pthread ${BOOST_LIB} Hyperspace DfsBroker AsyncComm Common log4cpp)

install (TARGETS Hyperspace.Master RUNTIME DESTINATION bin)

