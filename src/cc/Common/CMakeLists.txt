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

set (Common_SRCS
Checksum.cc
Error.cc
FileUtils.cc
InetAddr.cc
Init.cc
InteractiveCommand.cc
Logger.cc
Properties.cc
System.cc
Usage.cc
md5.cc
)

add_library (HyperCommon ${Common_SRCS})

add_executable (htConvert htConvert.cc)

# Link the executable to the HyperCommon library.
target_link_libraries (htConvert HyperCommon log4cpp pthread)

file (GLOB HEADERS *.h)

install (FILES ${HEADERS} DESTINATION include/hypertable-${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_MICRO}/Common)

install (TARGETS HyperCommon
         RUNTIME DESTINATION bin
         LIBRARY DESTINATION lib
         ARCHIVE DESTINATION lib)
