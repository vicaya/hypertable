#
# Copyright (C) 2008 Sriram Rao (Kosmix Corp)
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

include_directories ( ${Kfs_INCLUDE_DIR} )

# kosmosBroker
add_executable (kosmosBroker main.cc KosmosBroker.cc)
target_link_libraries (kosmosBroker pthread ${BOOST_LIB} HyperDfsBroker HyperComm HyperCommon log4cpp ${Kfs_LIBRARY})

install (TARGETS kosmosBroker RUNTIME DESTINATION bin)
