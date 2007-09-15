# - Find Log4cpp
# Find the native Log4cpp includes and library
#
#  Log4cpp_INCLUDE_DIR - where to find Log4cpp.h, etc.
#  Log4cpp_LIBRARIES   - List of libraries when using Log4cpp.
#  Log4cpp_FOUND       - True if Log4cpp found.


IF (Log4cpp_INCLUDE_DIR)
  # Already in cache, be silent
  SET(Log4cpp_FIND_QUIETLY TRUE)
ENDIF (Log4cpp_INCLUDE_DIR)

FIND_PATH(Log4cpp_INCLUDE_DIR log4cpp/Category.hh
  /opt/local/include
  /usr/local/include
  /usr/include
)

SET(Log4cpp_NAMES log4cpp)
FIND_LIBRARY(Log4cpp_LIBRARY
  NAMES ${Log4cpp_NAMES}
  PATHS /usr/lib /usr/local/lib /opt/local/lib
)

IF (Log4cpp_INCLUDE_DIR AND Log4cpp_LIBRARY)
   SET(Log4cpp_FOUND TRUE)
    SET( Log4cpp_LIBRARIES ${Log4cpp_LIBRARY} )
ELSE (Log4cpp_INCLUDE_DIR AND Log4cpp_LIBRARY)
   SET(Log4cpp_FOUND FALSE)
   SET( Log4cpp_LIBRARIES )
ENDIF (Log4cpp_INCLUDE_DIR AND Log4cpp_LIBRARY)

IF (Log4cpp_FOUND)
   IF (NOT Log4cpp_FIND_QUIETLY)
      MESSAGE(STATUS "Found Log4cpp: ${Log4cpp_LIBRARY}")
   ENDIF (NOT Log4cpp_FIND_QUIETLY)
ELSE (Log4cpp_FOUND)
   IF (Log4cpp_FIND_REQUIRED)
      MESSAGE(STATUS "Looked for Log4cpp libraries named ${Log4cppS_NAMES}.")
      MESSAGE(FATAL_ERROR "Could NOT find Log4cpp library")
   ENDIF (Log4cpp_FIND_REQUIRED)
ENDIF (Log4cpp_FOUND)

MARK_AS_ADVANCED(
  Log4cpp_LIBRARY
  Log4cpp_INCLUDE_DIR
  )
