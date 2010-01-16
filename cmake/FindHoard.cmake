# - Find Hoard
# Find the native Hoard includes and library
#
#  Hoard_LIBRARIES   - List of libraries when using Hoard.
#  Hoard_FOUND       - True if Hoard found.

set(Hoard_NAMES hoard)

find_library(Hoard_LIBRARY NO_DEFAULT_PATH
  NAMES ${Hoard_NAMES}
  PATHS ${HT_DEPENDENCY_LIB_DIR} /lib /usr/lib /usr/local/lib /opt/local/lib
)

if (Hoard_LIBRARY)
  set(Hoard_FOUND TRUE)
  set( Hoard_LIBRARIES ${Hoard_LIBRARY} )
else ()
  set(Hoard_FOUND FALSE)
  set( Hoard_LIBRARIES )
endif ()

if (Hoard_FOUND)
  message(STATUS "Found Hoard: ${Hoard_LIBRARY}")
else ()
  message(STATUS "Not Found Hoard: ${Hoard_LIBRARY}")
endif ()

mark_as_advanced(
  Hoard_LIBRARY
  )
