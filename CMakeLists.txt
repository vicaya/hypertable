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

project (HYPERTABLE)

set(VERSION_MAJOR "0")
set(VERSION_MINOR "9")
set(VERSION_MICRO "0")
set(VERSION_PATCH "5")
set(VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_MICRO}.${VERSION_PATCH})

# Enable testing
enable_testing()

if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)
  add_definitions(-Wall -pedantic -Wno-long-long)
endif (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)

add_definitions(-D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_REENTRANT)

# Uncomment the following to build dynamic libraries
# set (BUILD_SHARED_LIBS ON)

# Locate required packages
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
find_package(ZLIB REQUIRED)
find_package(Boost REQUIRED)
find_package(Kfs)
find_package(Log4cpp REQUIRED)
find_package(EXPAT REQUIRED)
find_package(Doxygen)
find_package(Tcmalloc)

# set malloc library
set( MALLOC_LIBRARY ${Tcmalloc_LIBRARIES} )

# include directories
include_directories(src/cc ${Boost_INCLUDE_DIRS} ${Log4cpp_INCLUDE_DIR})

if (NOT ${BOOST_CHECK} STREQUAL "0")
  message(FATAL_ERROR "Boost version not compatible")
endif (NOT ${BOOST_CHECK} STREQUAL "0")

if (${BOOST_VERSION} MATCHES "1_34")
  message(STATUS "Got boost 1.34.x, prepend fix directory")
  include_directories(BEFORE src/cc/boost-1_34-fix)
endif (${BOOST_VERSION} MATCHES "1_34")

if (${GCC_VERSION} MATCHES "^[4-9]\\.")
  add_definitions(-Wno-variadic-macros)
elseif (${GCC_VERSION} MATCHES "^3\\.")
  message(STATUS "Ancient gcc detected, hope it finishes despite warnings...")
endif (${GCC_VERSION} MATCHES "^[4-9]\\.")

# link directories
link_directories(${Boost_LIBRARY_DIRS})

IF (DOXYGEN_FOUND)
   configure_file(${HYPERTABLE_SOURCE_DIR}/doc/Doxyfile ${HYPERTABLE_BINARY_DIR}/Doxyfile)
   add_custom_target(doc ${DOXYGEN_EXECUTABLE} ${HYPERTABLE_BINARY_DIR}/Doxyfile)
ENDIF (DOXYGEN_FOUND)

# Recurse into the "Hello" and "Demo" subdirectories.  This does not actually
# cause another cmake executable to run.  The same process will walk through
# the project's entire directory structure.
add_subdirectory (src/cc/AsyncComm)
add_subdirectory (src/cc/Common)
add_subdirectory (src/cc/Hyperspace)
add_subdirectory (src/cc/Hypertable/Master)
add_subdirectory (src/cc/Hypertable/RangeServer)
add_subdirectory (src/cc/Hypertable/Lib)
add_subdirectory (src/cc/Tools/dfsclient)
add_subdirectory (src/cc/Tools/hyperspace)
add_subdirectory (src/cc/Tools/hypertable)
add_subdirectory (src/cc/Tools/dumplog)
add_subdirectory (src/cc/Tools/merge_diff)
add_subdirectory (src/cc/Tools/rsclient)
add_subdirectory (src/cc/Tools/rsdump)
add_subdirectory (src/cc/Tools/serverup)
add_subdirectory (src/cc/DfsBroker/Lib)
add_subdirectory (src/cc/DfsBroker/local)
#add_subdirectory (examples/simple_bench)
IF (Kfs_FOUND)
   add_subdirectory (src/cc/DfsBroker/kosmos)
ENDIF (Kfs_FOUND)

install (FILES demo/metadata-test.xml DESTINATION ${VERSION}/demo)
install (FILES demo/testdata.txt.bz2 DESTINATION ${VERSION}/demo)
install (FILES demo/Test.xml DESTINATION ${VERSION}/demo)
install (FILES demo/Test-schema.xml DESTINATION ${VERSION}/demo)
install (FILES conf/hypertable.cfg DESTINATION ${VERSION}/conf)
install (FILES conf/METADATA.xml DESTINATION ${VERSION}/conf)
install (FILES lib/commons-logging-1.0.4.jar DESTINATION ${VERSION}/java/lib)
install (FILES lib/hadoop-0.16.2-core.jar DESTINATION ${VERSION}/java/lib)
install (FILES lib/junit-4.3.1.jar DESTINATION ${VERSION}/java/lib)
install (FILES lib/log4j-1.2.13.jar DESTINATION ${VERSION}/java/lib)
install (FILES lib/hypertable-${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_MICRO}.${VERSION_PATCH}.jar DESTINATION ${VERSION}/java/lib)
install (FILES build/hypertable-${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_MICRO}.${VERSION_PATCH}.jar DESTINATION ${VERSION}/java/lib OPTIONAL)
install (FILES bin/jrun 
         PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	 DESTINATION ${VERSION}/bin)
install (FILES bin/start-all-servers.sh
         PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	 DESTINATION ${VERSION}/bin)
install (FILES bin/start-master.sh
         PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	 DESTINATION ${VERSION}/bin)
install (FILES bin/start-range-server.sh
         PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	 DESTINATION ${VERSION}/bin)
install (FILES bin/kill-servers.sh
         PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	 DESTINATION ${VERSION}/bin)
install (FILES bin/slaves.sh
         PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	 DESTINATION ${VERSION}/bin)
install (FILES test/create-test-tables.hql DESTINATION ${VERSION}/test)
install (DIRECTORY examples/apache_log DESTINATION ${VERSION}/examples USE_SOURCE_PERMISSIONS)
install (DIRECTORY examples/hql_tutorial DESTINATION ${VERSION}/examples USE_SOURCE_PERMISSIONS)

