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

set ( Master_SRCS
ConnectionHandler.cc
CreateDirectoryLayout.cc
Master.cc
RequestHandlerCreateTable.cc
RequestHandlerGetSchema.cc
RequestHandlerStatus.cc
ResponseCallbackGetSchema.cc
main.cc
)

# Hypertable.Master
add_executable (Hypertable.Master ${Master_SRCS})
target_link_libraries (Hypertable.Master pthread ${BOOST_LIB} Hypertable Hyperspace DfsBroker AsyncComm Common log4cpp expat)

install (TARGETS Hypertable.Master RUNTIME DESTINATION bin)
