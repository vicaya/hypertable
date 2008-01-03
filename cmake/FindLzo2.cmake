# - Try to find Lzo2
# Once done this will define
#
#  Lzo2_FOUND - system has Lzo2
#  Lzo2_INCLUDE_DIR - the Lzo2 include directory
#  Lzo2_LIBRARIES - Link these to use Lzo2
#  Lzo2_DEFINITIONS - Compiler switches required for using Lzo2
#  Lzo2_NEED_PREFIX - this is set if the functions are prefixed with BZ2_

# Copyright (c) 2006, Alexander Neundorf, <neundorf@kde.org>
#
# Redistribution and use is allowed according to the terms of the BSD license.
# For details see the accompanying COPYING-CMAKE-SCRIPTS file.


IF (Lzo2_INCLUDE_DIR AND Lzo2_LIBRARIES)
    SET(Lzo2_FIND_QUIETLY TRUE)
ENDIF (Lzo2_INCLUDE_DIR AND Lzo2_LIBRARIES)

FIND_PATH(Lzo2_INCLUDE_DIR lzo1x.h )

FIND_LIBRARY(Lzo2_LIBRARIES NAMES lzo2 )

IF (Lzo2_INCLUDE_DIR AND Lzo2_LIBRARIES)
   SET(Lzo2_FOUND TRUE)
#   INCLUDE(CheckLibraryExists)
#   CHECK_LIBRARY_EXISTS(${Lzo2_LIBRARIES} BZ2_bzCompressInit "" Lzo2_NEED_PREFIX)
ELSE (Lzo2_INCLUDE_DIR AND Lzo2_LIBRARIES)
   SET(Lzo2_FOUND FALSE)
ENDIF (Lzo2_INCLUDE_DIR AND Lzo2_LIBRARIES)

IF (Lzo2_FOUND)
  IF (NOT Lzo2_FIND_QUIETLY)
    MESSAGE(STATUS "Found Lzo2: ${Lzo2_LIBRARIES}")
  ENDIF (NOT Lzo2_FIND_QUIETLY)
ELSE (Lzo2_FOUND)
  IF (Lzo2_FIND_REQUIRED)
    MESSAGE(FATAL_ERROR "Could NOT find Lzo2")
  ENDIF (Lzo2_FIND_REQUIRED)
ENDIF (Lzo2_FOUND)

MARK_AS_ADVANCED(Lzo2_INCLUDE_DIR Lzo2_LIBRARIES)

