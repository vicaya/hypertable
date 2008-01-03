# - Find Tcmalloc
# Find the native Tcmalloc includes and library
#
#  Tcmalloc_INCLUDE_DIR - where to find Tcmalloc.h, etc.
#  Tcmalloc_LIBRARIES   - List of libraries when using Tcmalloc.
#  Tcmalloc_FOUND       - True if Tcmalloc found.


IF (Tcmalloc_INCLUDE_DIR)
  # Already in cache, be silent
  SET(Tcmalloc_FIND_QUIETLY TRUE)
ENDIF (Tcmalloc_INCLUDE_DIR)

FIND_PATH(Tcmalloc_INCLUDE_DIR google/heap-checker.h
  /opt/local/include
  /usr/local/include
  /usr/include
)

SET(Tcmalloc_NAMES tcmalloc_minimal)
FIND_LIBRARY(Tcmalloc_LIBRARY
  NAMES ${Tcmalloc_NAMES}
  PATHS /usr/lib /usr/local/lib /opt/local/lib
)

IF (Tcmalloc_INCLUDE_DIR AND Tcmalloc_LIBRARY)
   SET(Tcmalloc_FOUND TRUE)
    SET( Tcmalloc_LIBRARIES ${Tcmalloc_LIBRARY} )
ELSE (Tcmalloc_INCLUDE_DIR AND Tcmalloc_LIBRARY)
   SET(Tcmalloc_FOUND FALSE)
   SET( Tcmalloc_LIBRARIES )
ENDIF (Tcmalloc_INCLUDE_DIR AND Tcmalloc_LIBRARY)

IF (Tcmalloc_FOUND)
   IF (NOT Tcmalloc_FIND_QUIETLY)
      MESSAGE(STATUS "Found Tcmalloc: ${Tcmalloc_LIBRARY}")
   ENDIF (NOT Tcmalloc_FIND_QUIETLY)
      MESSAGE(STATUS "Found Tcmalloc: ${Tcmalloc_LIBRARY}")
ELSE (Tcmalloc_FOUND)
      MESSAGE(STATUS "Not Found Tcmalloc: ${Tcmalloc_LIBRARY}")
   IF (Tcmalloc_FIND_REQUIRED)
      MESSAGE(STATUS "Looked for Tcmalloc libraries named ${TcmallocS_NAMES}.")
      MESSAGE(FATAL_ERROR "Could NOT find Tcmalloc library")
   ENDIF (Tcmalloc_FIND_REQUIRED)
ENDIF (Tcmalloc_FOUND)

MARK_AS_ADVANCED(
  Tcmalloc_LIBRARY
  Tcmalloc_INCLUDE_DIR
  )
