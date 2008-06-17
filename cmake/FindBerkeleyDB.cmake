# -*- cmake -*-

# - Find BerkeleyDB
# Find the BerkeleyDB includes and library
# This module defines
#  BDB_INCLUDE_DIR, where to find db.h, etc.
#  BDB_LIBRARIES, the libraries needed to use BerkeleyDB.
#  BDB_FOUND, If false, do not try to use BerkeleyDB.
# also defined, but not for general use are
#  BDB_LIBRARY, where to find the BerkeleyDB library.

FILE(GLOB globbed_bdb_includes /usr/local/BerkeleyDB*/include)
FIND_PATH(BDB_INCLUDE_DIR db_cxx.h
    ${globbed_bdb_includes}
    /opt/local/include/db47
    /opt/local/include/db46     # introduced key_exists
    /usr/local/include/db4
    /usr/local/include
    /usr/include/db4
    /usr/include/)

FILE(GLOB globbed_bdb_libs /usr/local/BerkeleyDB*/lib)
SET(BDB_NAMES ${BDB_NAMES} db_cxx)
FIND_LIBRARY(BDB_LIBRARY
  NAMES ${BDB_NAMES}
  PATHS ${globbed_bdb_libs}
        /opt/local/lib/db47
        /opt/local/lib/db46     # ditto
        /usr/local/lib
        /usr/lib)

IF (BDB_LIBRARY AND BDB_INCLUDE_DIR)
    SET(BDB_LIBRARIES ${BDB_LIBRARY})
    SET(BDB_FOUND "YES")
ELSE (BDB_LIBRARY AND BDB_INCLUDE_DIR)
  SET(BDB_FOUND "NO")
ENDIF (BDB_LIBRARY AND BDB_INCLUDE_DIR)


IF (BDB_FOUND)
   IF (NOT BDB_FIND_QUIETLY)
      MESSAGE(STATUS "Found BerkeleyDB: ${BDB_LIBRARIES}")
   ENDIF (NOT BDB_FIND_QUIETLY)
ELSE (BDB_FOUND)
   IF (BDB_FIND_REQUIRED)
      MESSAGE(FATAL_ERROR "Could not find BerkeleyDB library")
   ENDIF (BDB_FIND_REQUIRED)
ENDIF (BDB_FOUND)

# Deprecated declarations.
SET (NATIVE_BDB_INCLUDE_PATH ${BDB_INCLUDE_DIR} )
GET_FILENAME_COMPONENT (NATIVE_BDB_LIB_PATH ${BDB_LIBRARY} PATH)

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
endif (NOT BDB_CHECK STREQUAL "0")

if (NOT BDB_VERSION MATCHES "^([4-9]|[1-9][0-9]+)\\.([6-9]|[1-9][0-9]+)")
  message(FATAL_ERROR "At least 4.6.x of BerkeleyDB is required. "
          "Please fix the installation, remove CMakeCache.txt and try again.")
endif (NOT BDB_VERSION MATCHES "^([4-9]|[1-9][0-9]+)\\.([6-9]|[1-9][0-9]+)")


MARK_AS_ADVANCED(
  BDB_LIBRARY
  BDB_INCLUDE_DIR
  )
