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

macro(FIND_KFS_LIB lib)
  find_library(${lib}_LIB NAMES ${lib}
    PATHS /opt/kfs/lib/static /opt/kfs/lib /opt/local/lib /usr/local/lib
          $ENV{HOME}/src/kosmosfs/build/lib/static
  )
endmacro(FIND_KFS_LIB lib libname)

FIND_KFS_LIB(KfsClient)
FIND_KFS_LIB(KfsIO)
FIND_KFS_LIB(KfsCommon)

if (Kfs_INCLUDE_DIR AND KfsClient_LIB)
  set(Kfs_FOUND TRUE)
  set( Kfs_LIBRARIES ${KfsClient_LIB} ${KfsIO_LIB} ${KfsCommon_LIB})
else (Kfs_INCLUDE_DIR AND KfsClient_LIBRARY)
   set(Kfs_FOUND FALSE)
   set( Kfs_LIBRARIES)
endif (Kfs_INCLUDE_DIR AND KfsClient_LIB)

if (Kfs_FOUND)
   if (NOT Kfs_FIND_QUIETLY)
      message(STATUS "Found KFS: ${Kfs_LIBRARIES}")
   endif (NOT Kfs_FIND_QUIETLY)
else (Kfs_FOUND)
   if (Kfs_FIND_REQUIRED)
      message(FATAL_ERROR "Could NOT find KFS libraries")
   endif (Kfs_FIND_REQUIRED)
endif (Kfs_FOUND)

mark_as_advanced(
  Kfs_INCLUDE_DIR
  KfsClient_LIB
  KfsIO_LIB
  KfsCommon_LIB
)
