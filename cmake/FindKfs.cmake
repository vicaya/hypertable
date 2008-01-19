# - Find Kfs
# Find the native Kfs includes and library
#
#  Kfs_INCLUDE_DIR - where to find Kfs.h, etc.
#  Kfs_LIBRARIES   - List of libraries when using Kfs.
#  Kfs_FOUND       - True if Kfs found.


IF (Kfs_INCLUDE_DIR)
  # Already in cache, be silent
  SET(Kfs_FIND_QUIETLY TRUE)
ENDIF (Kfs_INCLUDE_DIR)

FIND_PATH(Kfs_INCLUDE_DIR kfs/KfsClient.h
  /home/doug/src/kosmosfs/build/release/include
  /opt/local/include
  /usr/local/include
  /usr/include
)

SET(Kfs_NAMES kfsClient)
FIND_LIBRARY(Kfs_LIBRARY
  NAMES ${Kfs_NAMES}
  PATHS /home/doug/src/kosmosfs/build/release/lib /usr/lib /usr/local/lib /opt/local/lib
)

IF (Kfs_INCLUDE_DIR AND Kfs_LIBRARY)
   SET(Kfs_FOUND TRUE)
    SET( Kfs_LIBRARIES ${Kfs_LIBRARY} )
ELSE (Kfs_INCLUDE_DIR AND Kfs_LIBRARY)
   SET(Kfs_FOUND FALSE)
   SET( Kfs_LIBRARIES )
ENDIF (Kfs_INCLUDE_DIR AND Kfs_LIBRARY)

IF (Kfs_FOUND)
   IF (NOT Kfs_FIND_QUIETLY)
      MESSAGE(STATUS "Found KFS: ${Kfs_LIBRARY}")
   ENDIF (NOT Kfs_FIND_QUIETLY)
ELSE (Kfs_FOUND)
   IF (Kfs_FIND_REQUIRED)
      MESSAGE(STATUS "Looked for KFS libraries named ${Kfs_NAMES}.")
      MESSAGE(FATAL_ERROR "Could NOT find KFS library")
   ENDIF (Kfs_FIND_REQUIRED)
ENDIF (Kfs_FOUND)

MARK_AS_ADVANCED(
  Kfs_LIBRARY
  Kfs_INCLUDE_DIR
  )
