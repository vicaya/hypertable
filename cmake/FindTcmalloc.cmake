# - Find Tcmalloc
# Find the native Tcmalloc includes and library
#
#  Tcmalloc_INCLUDE_DIR - where to find Tcmalloc.h, etc.
#  Tcmalloc_LIBRARIES   - List of libraries when using Tcmalloc.
#  Tcmalloc_FOUND       - True if Tcmalloc found.


if (Tcmalloc_INCLUDE_DIR)
  # Already in cache, be silent
  set(Tcmalloc_FIND_QUIETLY TRUE)
endif ()

find_path(Tcmalloc_INCLUDE_DIR google/tcmalloc.h
  /opt/local/include
  /usr/local/include
)

set(Tcmalloc_NAMES tcmalloc_minimal tcmalloc)
find_library(Tcmalloc_LIBRARY
  NAMES ${Tcmalloc_NAMES}
  PATHS /usr/local/lib /opt/local/lib
)

if (Tcmalloc_INCLUDE_DIR AND Tcmalloc_LIBRARY)
  set(Tcmalloc_FOUND TRUE)
  set( Tcmalloc_LIBRARIES ${Tcmalloc_LIBRARY} )
else ()
  set(Tcmalloc_FOUND FALSE)
  set( Tcmalloc_LIBRARIES )
endif ()

if (Tcmalloc_FOUND)
  if (NOT Tcmalloc_FIND_QUIETLY)
    message(STATUS "Found Tcmalloc: ${Tcmalloc_LIBRARY}")
    try_run(TC_CHECK TC_CHECK_BUILD
            ${HYPERTABLE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeTmp
            ${HYPERTABLE_SOURCE_DIR}/cmake/CheckTcmalloc.cc
            CMAKE_FLAGS -DINCLUDE_DIRECTORIES=${Tcmalloc_INCLUDE_DIR}
                        -DLINK_LIBRARIES=${Tcmalloc_LIBRARIES}
            OUTPUT_VARIABLE TC_TRY_OUT)
    #message("tc_check build: ${TC_CHECK_BUILD}")
    #message("tc_check: ${TC_CHECK}")
    #message("tc_version: ${TC_TRY_OUT}")
    if (TC_CHECK_BUILD AND NOT TC_CHECK STREQUAL "0")
      string(REGEX REPLACE ".*\n(Tcmalloc .*)" "\\1" TC_TRY_OUT ${TC_TRY_OUT})
      message(STATUS "${TC_TRY_OUT}")
      message(FATAL_ERROR "Please fix the tcmalloc installation and try again.")
      set(Tcmalloc_LIBRARIES)
    endif ()
    string(REGEX REPLACE ".*\n([0-9]+[^\n]+).*" "\\1" TC_VERSION ${TC_TRY_OUT})
    if (NOT TC_VERSION MATCHES "^[0-9]+.*")
      set(TC_VERSION "unknown -- make sure it's 1.1+")
    endif ()
    message(STATUS "       version: ${TC_VERSION}")
  endif ()
else ()
  message(STATUS "Not Found Tcmalloc: ${Tcmalloc_LIBRARY}")
  if (Tcmalloc_FIND_REQUIRED)
    message(STATUS "Looked for Tcmalloc libraries named ${Tcmalloc_NAMES}.")
    message(FATAL_ERROR "Could NOT find Tcmalloc library")
  endif ()
endif ()

mark_as_advanced(
  Tcmalloc_LIBRARY
  Tcmalloc_INCLUDE_DIR
  )
