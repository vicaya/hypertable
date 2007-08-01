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

project (HYPERTABLE)

set (CMAKE_BUILD_TYPE, Debug)
set (CMAKE_INSTALL_PREFIX "/Users/doug/install")

# CMAKE_CXX_FLAGS
#set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-variadic-macros -Wall")

# Enable testing
enable_testing()

# Locate zlib
find_package(ZLIB REQUIRED)

# include directories
include_directories( /usr/local/include/boost-1_33_1 src/cc )

# Recurse into the "Hello" and "Demo" subdirectories.  This does not actually
# cause another cmake executable to run.  The same process will walk through
# the project's entire directory structure.
add_subdirectory (src/cc/AsyncComm)
add_subdirectory (src/cc/Common)
add_subdirectory (src/cc/Hyperspace)
add_subdirectory (src/cc/HdfsClient)
add_subdirectory (src/cc/Hypertable)
add_subdirectory (src/cc/Hypertable/Test)
add_subdirectory (src/cc/Hypertable/Master)
add_subdirectory (src/cc/Hypertable/RangeServer)

install (FILES demo/metadata-test.xml DESTINATION demo)
install (FILES demo/testdata.txt.bz2 DESTINATION demo)
install (FILES demo/Test.xml DESTINATION demo)
install (FILES conf/hypertable.cfg DESTINATION conf)
install (FILES lib/commons-logging-1.0.4.jar DESTINATION java/lib)
install (FILES lib/hadoop-0.12.2-core.jar DESTINATION java/lib)
install (FILES lib/junit-4.3.1.jar DESTINATION java/lib)
install (FILES lib/log4j-1.2.13.jar DESTINATION java/lib)
install (FILES build/hypertable.jar DESTINATION java/lib)
install (FILES bin/jrun 
         PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	 DESTINATION bin)
install (FILES bin/start-servers.sh
         PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	 DESTINATION bin)
