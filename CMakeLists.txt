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

# Hypertable version components
set(VERSION_MAJOR "0")
set(VERSION_MINOR "9")
set(VERSION_MICRO "0")
set(VERSION_PATCH "8")

# cmake behavior compatibility
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

# install directory prefix
if (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX "/opt/hypertable"
      CACHE PATH "installation directory prefix" FORCE)
endif (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)

# A more informative hypertable version string
set(VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_MICRO}.${VERSION_PATCH})
exec_program(git ${HYPERTABLE_SOURCE_DIR} ARGS describe --long
             OUTPUT_VARIABLE HT_GIT_VERSION RETURN_VALUE GIT_RETURN)
if (GIT_RETURN STREQUAL "0")
  set(HT_VCS_STRING ${HT_GIT_VERSION})
else (GIT_RETURN STREQUAL "0")
  set(HT_VCS_STRING "release tarball")
endif (GIT_RETURN STREQUAL "0")
set(VERSION_STRING "Hypertable ${VERSION} (${HT_VCS_STRING})")

# Enable testing
enable_testing()

# gcc warning settings
if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)
  add_definitions(-Wall -pedantic -Wno-long-long)
endif (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)

add_definitions(-D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_REENTRANT)

# Uncomment the following to build dynamic libraries
# set (BUILD_SHARED_LIBS ON)

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
find_package(JNI)
find_package(PythonLibs)

if (PYTHON_LIBS_FOUND)
  message (STATUS "Python libraries found at : ${PYTHON_LIBS_FOUND}")
endif (PYTHON_LIBS_FOUND)
if (JAVA_INCLUDE_PATH)
  message(STATUS "Java headers found at: ${JAVA_INCLUDE_PATH}")
  if (HADOOP_INCLUDE_PATH)
    message(STATUS "Hadoop includes located at: ${HADOOP_INCLUDE_PATH}")
  else (HADOOP_INCLUDE_PATH)
    message(STATUS "Hadoop includes not found. Please set HADOOP_INCLUDE_PATH variable")
  endif(HADOOP_INCLUDE_PATH)
  
  if (HADOOP_LIB_PATH)
    message(STATUS "Hadoop libraries located at: ${HADOOP_LIB_PATH}")
  else (HADOOP_LIB_PATH)
    message(STATUS "Hadoop libraries not found. Please set HADOOP_LIB_PATH variable")
  endif (HADOOP_LIB_PATH)
  
  if (NOT BUILD_SHARED_LIBS)
    message(STATUS "You decided not to build shared libraries. MapReduce will be disabled")
  endif (NOT BUILD_SHARED_LIBS)
else (JAVA_INCLUDE_PATH)
  message(STATUS "Java JNI not found. Mapreduce support will be disabled.")
endif(JAVA_INCLUDE_PATH)

if (JAVA_INCLUDE_PATH AND HADOOP_LIB_PATH AND HADOOP_INCLUDE_PATH AND BUILD_SHARED_LIBS)
  set(BUILD_MAPREDUCE ON)
endif (JAVA_INCLUDE_PATH AND HADOOP_LIB_PATH AND HADOOP_INCLUDE_PATH AND BUILD_SHARED_LIBS)

# set malloc library
set(MALLOC_LIBRARY ${Tcmalloc_LIBRARIES})

# include directories
include_directories(src/cc ${HYPERTABLE_BINARY_DIR}/src/cc
    ${ZLIB_INCLUDE_DIR} ${Boost_INCLUDE_DIRS} ${Log4cpp_INCLUDE_DIR}
    ${EXPAT_INCLUDE_DIRS} ${BDB_INCLUDE_DIR} ${READLINE_INCLUDE_DIR})

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
add_subdirectory(src/cc/Tools)
add_subdirectory(src/cc/DfsBroker/Lib)
add_subdirectory(src/cc/DfsBroker/local)
add_subdirectory(src/cc/Benchmark/random)
add_subdirectory(examples)

if (BUILD_MAPREDUCE)
  add_subdirectory(contrib/cc/MapReduce)
else (BUILD_MAPREDUCE)
  message(STATUS "MapReduce support disabled.")
endif (BUILD_MAPREDUCE)
#add_subdirectory(contrib/cc/PythonBinding)

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
install(FILES lib/hadoop-0.17.1-core.jar DESTINATION ${VERSION}/java/lib)
install(FILES lib/junit-4.3.1.jar DESTINATION ${VERSION}/java/lib)
install(FILES lib/log4j-1.2.13.jar DESTINATION ${VERSION}/java/lib)
install(FILES lib/hypertable-${VERSION}.jar DESTINATION ${VERSION}/java/lib)
install(FILES build/hypertable-${VERSION}.jar DESTINATION ${VERSION}/java/lib
        OPTIONAL)
install(PROGRAMS bin/jrun bin/clean-database.sh bin/start-dfsbroker.sh
		 bin/start-master.sh bin/start-hyperspace.sh
		 bin/start-rangeserver.sh bin/start-all-servers.sh
		 bin/stop-servers.sh bin/slaves.sh
	DESTINATION ${VERSION}/bin)
install(FILES test/create-test-tables.hql DESTINATION ${VERSION}/test)
install(DIRECTORY examples/apache_log DESTINATION ${VERSION}/examples
        USE_SOURCE_PERMISSIONS)
install(DIRECTORY examples/hql_tutorial DESTINATION ${VERSION}/examples
        USE_SOURCE_PERMISSIONS)
