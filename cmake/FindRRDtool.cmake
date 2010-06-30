# -*- cmake -*-

# - Find RRDtool 
# Find the RRDtool includes and library
# This module defines
#  RRD_INCLUDE_DIR, where to find rrd.h, etc.
#  RRD_LIBRARIES, the libraries needed to use RRDtool.
#  RRD_FOUND, If false, do not try to use RRDtool.
#  also defined, RD_LIBRARY, where to find the RRDtool library.

find_path(RRD_INCLUDE_DIR rrd.h NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_INCLUDE_DIR}
    /opt/local/include
    /usr/local/include
    /usr/include
    )

set(RRD_NAMES ${RRD_NAMES} rrd)
find_library(RRD_LIBRARY NAMES ${RRD_NAMES} NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib
    /usr/lib64
    )

find_library(FREETYPE_LIBRARY freetype PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib
    /usr/lib64
    )

find_library(PNG12_LIBRARY NAMES png12 NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib
    /usr/lib64
    )

find_library(ART_LGPL_2_LIBRARY NAMES art_lgpl_2 NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib
    /usr/lib64
    )

message(STATUS "${RRD_LIBRARY} ${RRD_INCLUDE_DIR} ${FREETYPE_LIBRARY} ${PNG12_LIBRARY} ${ART_LGPL_2_LIBRARY}")
  
if (RRD_LIBRARY AND RRD_INCLUDE_DIR AND FREETYPE_LIBRARY AND PNG12_LIBRARY AND ART_LGPL_2_LIBRARY)
  set(RRD_LIBRARIES ${RRD_LIBRARY} ${FREETYPE_LIBRARY} ${PNG12_LIBRARY} ${ART_LGPL_2_LIBRARY})
  set(RRD_FOUND "YES")
  message(STATUS "Found RRDtool: ${RRD_LIBRARIES}")
else ()
  set(RRD_FOUND "NO")
endif ()


if (RRD_FOUND)
  if (NOT RRD_FIND_QUIETLY)
    message(STATUS "Found RRDtool: ${RRD_LIBRARIES}")
  endif ()
else ()
  if (RRD_FIND_REQUIRED)
    message(FATAL_ERROR "Could not find RRDtool library")
  endif ()
endif ()

try_run(RRD_CHECK SHOULD_COMPILE
        ${HYPERTABLE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeTmp
        ${HYPERTABLE_SOURCE_DIR}/cmake/CheckRRDtool.cc
        CMAKE_FLAGS -DINCLUDE_DIRECTORIES=${RRD_INCLUDE_DIR}
                    -DLINK_LIBRARIES=${RRD_LIBRARY}
        OUTPUT_VARIABLE RRD_TRY_OUT)
string(REGEX REPLACE ".*\n([0-9.]+).*" "\\1" RRD_VERSION ${RRD_TRY_OUT})
string(REGEX REPLACE ".*\n(RRDtool .*)" "\\1" RRD_VERSION ${RRD_VERSION})
message(STATUS "RRDtool version: ${RRD_VERSION}")

if (NOT RRD_CHECK STREQUAL "0")
  message(FATAL_ERROR "Please fix the RRDtool installation, "
          "remove CMakeCache.txt and try again.")
endif ()

STRING(REGEX REPLACE "^([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\1" MAJOR "${RRD_VERSION}")
STRING(REGEX REPLACE "^([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\2" MINOR "${RRD_VERSION}")
STRING(REGEX REPLACE "^([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\3" PATCH "${RRD_VERSION}")

set(RRD_COMPATIBLE "YES")

if (NOT RRD_COMPATIBLE)
  message(FATAL_ERROR "RRD tool version >= required." 
          "Found version ${MAJOR}.${MINOR}.${PATCH}"
          "Please fix the installation, remove CMakeCache.txt and try again.")
endif()

mark_as_advanced(
  RRD_LIBRARIES
  RRD_INCLUDE_DIR
  )
