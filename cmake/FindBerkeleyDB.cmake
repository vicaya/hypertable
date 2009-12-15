# -*- cmake -*-

# - Find BerkeleyDB
# Find the BerkeleyDB includes and library
# This module defines
#  BDB_INCLUDE_DIR, where to find db.h, etc.
#  BDB_LIBRARIES, the libraries needed to use BerkeleyDB.
#  BDB_FOUND, If false, do not try to use BerkeleyDB.
#  also defined, but not for general use are
#  BDB_LIBRARY, where to find the BerkeleyDB library.

find_path(BDB_INCLUDE_DIR db_cxx.h NO_DEFAULT_PATH PATHS
    /usr/local/BerkeleyDB.4.8/include
    /usr/local/BerkeleyDB.4.7/include
    /opt/local/include/db48
    /opt/local/include/db47
    /opt/local/include/db46     # introduced key_exists
    /usr/local/include/db4
    /usr/local/include
    /usr/include/db4
    /usr/include
    )

set(BDB_NAMES ${BDB_NAMES} db_cxx)
find_library(BDB_LIBRARY NAMES ${BDB_NAMES} NO_DEFAULT_PATH PATHS
    /usr/local/BerkeleyDB.4.8/lib
    /usr/local/BerkeleyDB.4.7/lib
    /opt/local/lib/db48
    /opt/local/lib/db47
    /opt/local/lib/db46         # ditto
    /usr/local/lib/db4
    /usr/local/lib
    /usr/lib
    )

if (BDB_LIBRARY AND BDB_INCLUDE_DIR)
  set(BDB_LIBRARIES ${BDB_LIBRARY})
  set(BDB_FOUND "YES")
else ()
  set(BDB_FOUND "NO")
endif ()


if (BDB_FOUND)
  if (NOT BDB_FIND_QUIETLY)
    message(STATUS "Found BerkeleyDB: ${BDB_LIBRARIES}")
  endif ()
else ()
  if (BDB_FIND_REQUIRED)
    message(FATAL_ERROR "Could not find BerkeleyDB library")
  endif ()
endif ()

try_run(BDB_CHECK SHOULD_COMPILE
        ${HYPERTABLE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeTmp
        ${HYPERTABLE_SOURCE_DIR}/cmake/CheckBdb.cc
        CMAKE_FLAGS -DINCLUDE_DIRECTORIES=${BDB_INCLUDE_DIR}
                    -DLINK_LIBRARIES=${BDB_LIBRARIES}
        OUTPUT_VARIABLE BDB_TRY_OUT)
string(REGEX REPLACE ".*\n([0-9.]+).*" "\\1" BDB_VERSION ${BDB_TRY_OUT})
string(REGEX REPLACE ".*\n(BerkeleyDB .*)" "\\1" BDB_VERSION ${BDB_VERSION})
message(STATUS "Berkeley DB version: ${BDB_VERSION}")

if (NOT BDB_CHECK STREQUAL "0")
  message(FATAL_ERROR "Please fix the Berkeley DB installation, "
          "remove CMakeCache.txt and try again.")
endif ()

if (NOT BDB_VERSION MATCHES "^([4-9]|[1-9][0-9]+)\\.([6-9]|[1-9][0-9]+)")
  message(FATAL_ERROR "At least 4.6.x of BerkeleyDB is required. "
          "Please fix the installation, remove CMakeCache.txt and try again.")
endif ()


mark_as_advanced(
  BDB_LIBRARY
  BDB_INCLUDE_DIR
  )
