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

project(HYPERTABLE)

set(VERSION_MAJOR "0")
set(VERSION_MINOR "9")
set(VERSION_MICRO "0")
set(VERSION_PATCH "7")
set(VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_MICRO}.${VERSION_PATCH})

# Behavior compatibility
mark_as_advanced(CMAKE_BACKWARDS_COMPATIBILITY)
cmake_minimum_required(VERSION 2.4.6)
if (COMMAND cmake_policy)
  # we prefer the more strict behavior, to find out more:
  # cmake --help-policy CMP0003
  cmake_policy(SET CMP0003 NEW)
endif (COMMAND cmake_policy)

# Something nice for GUI like ccmake
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING
      "Options: None Debug Release RelWithDebInfo MinSizeRel." FORCE)
endif (NOT CMAKE_BUILD_TYPE)

# installprefix
if (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX "/opt/hypertable"
      CACHE PATH "installation directory prefix" FORCE)
endif (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)

# Enable testing
enable_testing()

# gcc warning settings
if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)
  add_definitions(-Wall -pedantic -Wno-long-long)
endif (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)

add_definitions(-D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_REENTRANT)

# Uncomment the following to build dynamic libraries
set (BUILD_SHARED_LIBS ON)

# Locate required packages
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
find_package(Threads REQUIRED)
message(STATUS "Use thread library: ${CMAKE_THREAD_LIBS_INIT}")
find_package(ZLIB REQUIRED)
find_package(Boost REQUIRED)
find_package(Kfs)
find_package(Log4cpp REQUIRED)
find_package(EXPAT REQUIRED)
find_package(Doxygen)
find_package(Tcmalloc)
find_package(BerkeleyDB REQUIRED)
find_package(Readline REQUIRED)

# set malloc library
set(MALLOC_LIBRARY ${Tcmalloc_LIBRARIES})

# include directories
include_directories(src/cc ${ZLIB_INCLUDE_DIR} ${Boost_INCLUDE_DIRS}
    ${Log4cpp_INCLUDE_DIR} ${EXPAT_INCLUDE_DIRS} ${BDB_INCLUDE_DIR})

if (BOOST_VERSION MATCHES "1_34")
  message(STATUS "Got boost 1.34.x, prepend fix directory")
  include_directories(BEFORE src/cc/boost-1_34-fix)
endif (BOOST_VERSION MATCHES "1_34")

if (GCC_VERSION MATCHES "^([4-9]|[1-9][0-9]+)\\.")
  add_definitions(-Wno-variadic-macros)
elseif (GCC_VERSION MATCHES "^([4-9]|[1-9][0-9]+)\\.")
  message(STATUS "Ancient gcc detected, hope it finishes despite warnings...")
endif (GCC_VERSION MATCHES "^([4-9]|[1-9][0-9]+)\\.")

if (DOXYGEN_FOUND)
  configure_file(${HYPERTABLE_SOURCE_DIR}/doc/Doxyfile
                 ${HYPERTABLE_BINARY_DIR}/Doxyfile)
  add_custom_target(doc ${DOXYGEN_EXECUTABLE} ${HYPERTABLE_BINARY_DIR}/Doxyfile)
endif (DOXYGEN_FOUND)

# sub-project directories
add_subdirectory(src/cc/Common)
add_subdirectory(src/cc/AsyncComm)
add_subdirectory(src/cc/Hyperspace)
add_subdirectory(src/cc/Hypertable/Master)
add_subdirectory(src/cc/Hypertable/RangeServer)
add_subdirectory(src/cc/Hypertable/Lib)
add_subdirectory(src/cc/Tools/bdbtest)
add_subdirectory(src/cc/Tools/dfsclient)
add_subdirectory(src/cc/Tools/hyperspace)
add_subdirectory(src/cc/Tools/hypertable)
add_subdirectory(src/cc/Tools/dumplog)
add_subdirectory(src/cc/Tools/merge_diff)
add_subdirectory(src/cc/Tools/rsclient)
add_subdirectory(src/cc/Tools/rsdump)
add_subdirectory(src/cc/Tools/serverup)
add_subdirectory(src/cc/DfsBroker/Lib)
add_subdirectory(src/cc/DfsBroker/local)
add_subdirectory(contrib)
#add_subdirectory(examples/simple_bench)
IF (Kfs_FOUND)
   add_subdirectory(src/cc/DfsBroker/kosmos)
ENDIF (Kfs_FOUND)

install(FILES demo/metadata-test.xml DESTINATION ${VERSION}/demo)
install(FILES demo/testdata.txt.bz2 DESTINATION ${VERSION}/demo)
install(FILES demo/Test.xml DESTINATION ${VERSION}/demo)
install(FILES demo/Test-schema.xml DESTINATION ${VERSION}/demo)
install(FILES conf/hypertable.cfg DESTINATION ${VERSION}/conf)
install(FILES conf/METADATA.xml DESTINATION ${VERSION}/conf)
install(FILES lib/commons-logging-1.0.4.jar DESTINATION ${VERSION}/java/lib)
install(FILES lib/hadoop-0.16.4-core.jar DESTINATION ${VERSION}/java/lib)
install(FILES lib/junit-4.3.1.jar DESTINATION ${VERSION}/java/lib)
install(FILES lib/log4j-1.2.13.jar DESTINATION ${VERSION}/java/lib)
install(FILES lib/hypertable-${VERSION}.jar DESTINATION ${VERSION}/java/lib)
install(FILES build/hypertable-${VERSION}.jar DESTINATION ${VERSION}/java/lib
        OPTIONAL)
install(PROGRAMS bin/jrun bin/start-master.sh bin/start-all-servers.sh
                 bin/start-range-server.sh bin/kill-servers.sh bin/slaves.sh
	DESTINATION ${VERSION}/bin)
install(FILES test/create-test-tables.hql DESTINATION ${VERSION}/test)
install(DIRECTORY examples/apache_log DESTINATION ${VERSION}/examples
        USE_SOURCE_PERMISSIONS)
install(DIRECTORY examples/hql_tutorial DESTINATION ${VERSION}/examples
        USE_SOURCE_PERMISSIONS)
