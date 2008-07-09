# - Find Kfs
# Find the native Kfs includes and library
#
#  Kfs_INCLUDE_DIR - where to find Kfs.h, etc.
#  Kfs_LIBRARIES   - List of libraries when using Kfs.
#  Kfs_FOUND       - True if Kfs found.


if (Kfs_INCLUDE_DIR)
  # Already in cache, be silent
  set(Kfs_FIND_QUIETLY TRUE)
endif (Kfs_INCLUDE_DIR)

find_path(Kfs_INCLUDE_DIR kfs/KfsClient.h
  /opt/kfs/include
  /opt/local/include
  /usr/local/include
  $ENV{HOME}/src/kosmosfs/build/include
)

set(Kfs_NAMES kfsClient)
find_library(Kfs_LIBRARY
  NAMES ${Kfs_NAMES}
  PATHS /opt/kfs/lib /opt/local/lib /usr/local/lib $ENV{HOME}/src/kosmosfs/build/lib/static
)

if (Kfs_INCLUDE_DIR AND Kfs_LIBRARY)
   set(Kfs_FOUND TRUE)
    set( Kfs_LIBRARIES ${Kfs_LIBRARY})
else (Kfs_INCLUDE_DIR AND Kfs_LIBRARY)
   set(Kfs_FOUND FALSE)
   set( Kfs_LIBRARIES)
endif (Kfs_INCLUDE_DIR AND Kfs_LIBRARY)

if (Kfs_FOUND)
   if (NOT Kfs_FIND_QUIETLY)
      message(STATUS "Found KFS: ${Kfs_LIBRARY}")
   endif (NOT Kfs_FIND_QUIETLY)
else (Kfs_FOUND)
   if (Kfs_FIND_REQUIRED)
      message(STATUS "Looked for KFS libraries named ${Kfs_NAMES}.")
      message(FATAL_ERROR "Could NOT find KFS library")
   endif (Kfs_FIND_REQUIRED)
endif (Kfs_FOUND)

mark_as_advanced(
  Kfs_LIBRARY
  Kfs_INCLUDE_DIR
)
