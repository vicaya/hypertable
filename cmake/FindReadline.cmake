# Find the Readline includes and libraries
# This module defines
# READLINE_INCLUDE_DIR, where to find db.h, etc.
# READLINE_LIBRARIES, the libraries needed to use BerkeleyDB.
# READLINE_FOUND, If false, do not try to use BerkeleyDB.
# also defined, but not for general use are
# READLINE_LIBRARY, NCURSES_LIBRARY

find_path(READLINE_INCLUDE_DIR readline/readline.h
    /opt/local/include
    /usr/local/include
    )

find_library(READLINE_LIBRARY NAMES readline PATHS
    /opt/local/lib
    /usr/local/lib
    )

find_library(NCURSES_LIBRARY NAMES ncurses PATHS
    /opt/local/lib
    /usr/local/lib
    )

if (READLINE_LIBRARY AND READLINE_INCLUDE_DIR AND NCURSES_LIBRARY)
  set(READLINE_LIBRARIES ${READLINE_LIBRARY} ${NCURSES_LIBRARY})
  set(READLINE_FOUND "YES")
else ()
  set(READLINE_FOUND "NO")
endif ()

if (READLINE_FOUND)
  if (NOT READLINE_FIND_QUIETLY)
    message(STATUS "Found Readline libraries: ${READLINE_LIBRARIES}")
  endif ()
else ()
  if (READLINE_FIND_REQUIRED)
    message(FATAL_ERROR "Could not find suitable Readline libraries")
  endif ()
endif ()

mark_as_advanced(
  NCURSES_LIBRARY
  READLINE_INCLUDE_DIR
  READLINE_LIBRARY
  )
