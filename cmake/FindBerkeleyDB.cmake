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
/usr/local/include/db4
/usr/local/include
/usr/include/db4
/usr/include/
)

FILE(GLOB globbed_bdb_libs /usr/local/BerkeleyDB*/lib)
SET(BDB_NAMES ${BDB_NAMES} db_cxx)
FIND_LIBRARY(BDB_LIBRARY
  NAMES ${BDB_NAMES}
  PATHS ${globbed_bdb_libs} /usr/lib /usr/local/lib
  )

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

MARK_AS_ADVANCED(
  BDB_LIBRARY
  BDB_INCLUDE_DIR
  )