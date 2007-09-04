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

set (CMAKE_INSTALL_PREFIX "/Users/doug/hypertable")

# CMAKE_CXX_FLAGS
#set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-variadic-macros -Wall")

# Enable testing
enable_testing()

# Uncomment the following to build dynamic libraries
# set (BUILD_SHARED_LIBS ON)

# Locate zlib
find_package(ZLIB REQUIRED)
find_package(Boost REQUIRED)

# include directories
include_directories( ${Boost_INCLUDE_DIRS} src/cc )

# link directories
link_directories(${Boost_LIBRARY_DIRS})

IF(EXISTS "${Boost_LIBRARY_DIR}/libboost_thread.a")
  set (BOOST_LIB "boost_thread")
ELSEIF(EXISTS "${Boost_LIBRARY_DIR}/libboost_thread-mt.a")
  set (BOOST_LIB "boost_thread-mt")
ELSE(EXISTS "${Boost_LIBRARY_DIR}/libboost_thread-gcc41-mt.a")
  set (BOOST_LIB "boost_thread-gcc41-mt")
ENDIF(EXISTS "${Boost_LIBRARY_DIR}/libboost_thread.a")

# Recurse into the "Hello" and "Demo" subdirectories.  This does not actually
# cause another cmake executable to run.  The same process will walk through
# the project's entire directory structure.
add_subdirectory (src/cc/AsyncComm)
add_subdirectory (src/cc/Common)
add_subdirectory (src/cc/Hyperspace)
add_subdirectory (src/cc/Hypertable/Master)
add_subdirectory (src/cc/Hypertable/RangeServer)
add_subdirectory (src/cc/Hypertable/Lib)
add_subdirectory (src/cc/Tools/hypertable)
add_subdirectory (src/cc/Tools/rsclient)
add_subdirectory (src/cc/Tools/serverup)
add_subdirectory (src/cc/DfsBroker/Lib)
add_subdirectory (src/cc/DfsBroker/Tools/dfstool)
add_subdirectory (src/cc/DfsBroker/local)

install (FILES demo/metadata-test.xml DESTINATION demo)
install (FILES demo/testdata.txt.bz2 DESTINATION demo)
install (FILES demo/Test.xml DESTINATION demo)
install (FILES demo/Test-schema.xml DESTINATION demo)
install (FILES conf/hypertable.cfg DESTINATION conf)
install (FILES lib/commons-logging-1.0.4.jar DESTINATION java/lib)
install (FILES lib/hadoop-0.13.1-core.jar DESTINATION java/lib)
install (FILES lib/junit-4.3.1.jar DESTINATION java/lib)
install (FILES lib/log4j-1.2.13.jar DESTINATION java/lib)
install (FILES build/hypertable.jar DESTINATION java/lib)
install (FILES bin/jrun 
         PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	 DESTINATION bin)
install (FILES bin/start-servers.sh
         PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	 DESTINATION bin)
install (FILES bin/stop-servers.sh
         PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	 DESTINATION bin)
install (FILES bin/initialize.sh
         PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	 DESTINATION bin)
	 