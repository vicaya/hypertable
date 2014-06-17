# - Find Log4cpp
# Find the native Log4cpp includes and library
#
#  Log4cpp_INCLUDE_DIR - where to find Log4cpp.h, etc.
#  Log4cpp_LIBRARIES   - List of libraries when using Log4cpp.
#  Log4cpp_FOUND       - True if Log4cpp found.

if (Log4cpp_INCLUDE_DIR)
  # Already in cache, be silent
  set(Log4cpp_FIND_QUIETLY TRUE)
endif ()

find_path(Log4cpp_INCLUDE_DIR log4cpp/Category.hh NO_DEFAULT_PATH PATHS
  ${HT_DEPENDENCY_INCLUDE_DIR}
  /opt/local/include
  /usr/local/include
  /usr/include
)

set(Log4cpp_NAMES log4cpp)
find_library(Log4cpp_LIBRARY NO_DEFAULT_PATH
  NAMES ${Log4cpp_NAMES}
  PATHS ${HT_DEPENDENCY_LIB_DIR} /usr/lib /usr/local/lib /opt/local/lib
)

if (Log4cpp_INCLUDE_DIR AND Log4cpp_LIBRARY)
  set(Log4cpp_FOUND TRUE)
  set( Log4cpp_LIBRARIES ${Log4cpp_LIBRARY} )
else ()
  set(Log4cpp_FOUND FALSE)
  set( Log4cpp_LIBRARIES )
endif ()

if (Log4cpp_FOUND)
  if (NOT Log4cpp_FIND_QUIETLY)
    message(STATUS "Found Log4cpp: ${Log4cpp_LIBRARIES}")
  endif ()
else ()
  if (Log4cpp_FIND_REQUIRED)
    message(STATUS "Looked for Log4cpp libraries named ${Log4cppS_NAMES}.")
    message(FATAL_ERROR "Could NOT find Log4cpp library")
  endif ()
endif ()

mark_as_advanced(
  Log4cpp_LIBRARY
  Log4cpp_INCLUDE_DIR
  )
