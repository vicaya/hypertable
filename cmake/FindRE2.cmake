# - Find RE2 
# Find the native RE2 includes and library
#
#  RE2_INCLUDE_DIR - where to find re2.h, etc.
#  RE2_LIBRARIES   - List of libraries when using RE2.
#  RE2_FOUND       - True if RE2 found.

find_path(RE2_INCLUDE_DIR re2/re2.h NO_DEFAULT_PATH PATHS
  ${HT_DEPENDENCY_INCLUDE_DIR}
  /usr/include
  /opt/local/include
  /usr/local/include
)

set(RE2_NAMES ${RE2_NAMES} re2)
find_library(RE2_LIBRARY NAMES ${RE2_NAMES} NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /usr/local/lib
    /usr/local/re2/lib
    /opt/local/lib
    /usr/lib
    )

if (RE2_INCLUDE_DIR AND RE2_LIBRARY)
  set(RE2_FOUND TRUE)
  set( RE2_LIBRARIES ${RE2_LIBRARY} )
else ()
  set(RE2_FOUND FALSE)
  set( RE2_LIBRARIES )
endif ()

if (RE2_FOUND)
  message(STATUS "Found RE2: ${RE2_LIBRARY}")
  try_run(RE2_CHECK RE2_CHECK_BUILD
          ${HYPERTABLE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeTmp
          ${HYPERTABLE_SOURCE_DIR}/cmake/CheckRE2.cc
          CMAKE_FLAGS -DINCLUDE_DIRECTORIES=${RE2_INCLUDE_DIR}
                      -DLINK_LIBRARIES=${RE2_LIBRARIES}
          OUTPUT_VARIABLE RE2_TRY_OUT)
  if (RE2_CHECK_BUILD AND NOT RE2_CHECK STREQUAL "0")
    string(REGEX REPLACE ".*\n(RE2 .*)" "\\1" RE2_TRY_OUT ${RE2_TRY_OUT})
    message(STATUS "${RE2_TRY_OUT}")
    message(FATAL_ERROR "Please fix the re2 installation and try again.")
    set(RE2_LIBRARIES)
  endif ()
  string(REGEX REPLACE ".*\n([0-9]+[^\n]+).*" "\\1" RE2_VERSION ${RE2_TRY_OUT})
  if (NOT RE2_VERSION MATCHES "^[0-9]+.*")
    set(RE2_VERSION "unknown") 
  endif ()
  message(STATUS "       version: ${RE2_VERSION}")
else ()
  message(STATUS "Not Found RE2: ${RE2_LIBRARY}")
  if (RE2_FIND_REQUIRED)
    message(STATUS "Looked for RE2 libraries named ${RE2_NAMES}.")
    message(FATAL_ERROR "Could NOT find RE2 library")
  endif ()
endif ()

mark_as_advanced(
  RE2_LIBRARY
  RE2_INCLUDE_DIR
  )
