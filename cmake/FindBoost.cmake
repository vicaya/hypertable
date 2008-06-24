# - Find the Boost includes and libraries.
# The following variables are set if Boost is found.  If Boost is not
# found, Boost_FOUND is set to false.
#  Boost_FOUND        - True when the Boost include directory is found.
#  Boost_INCLUDE_DIRS - the path to where the boost include files are.
#  Boost_LIBRARY_DIRS - The path to where the boost library files are.
#  Boost_LIB_DIAGNOSTIC_DEFINITIONS - Only set if using Windows.
#  BOOST_LIBS - Our default boost libs
#  BOOST_THREAD_LIB - The name of the boost thread library
#  BOOST_PROGRAM_OPTIONS_LIB - The name of the boost program options library
#  BOOST_IOSTREAMS_LIB - The name of the boost program options library
# ----------------------------------------------------------------------------
#
# Usage:
# In your CMakeLists.txt file do something like this:
# ...
# # Boost
# find_package(Boost)
# ...
# include_directories(${Boost_INCLUDE_DIRS})

if (NOT Boost_FIND_QUIETLY)
  message(STATUS "Looking for required boost libraries...")
endif (NOT Boost_FIND_QUIETLY)

if (WIN32)
  set(Boost_LIB_DIAGNOSTIC_DEFINITIONS "-DBOOST_LIB_DIAGNOSTIC")
endif (WIN32)

set(BOOST_INCLUDE_PATH_DESCRIPTION "directory containing the boost include "
    "files. E.g /usr/local/include/boost-1_34_1 or "
    "c:\\boost\\include\\boost-1_34_1")

set(BOOST_DIR_MESSAGE "Set the Boost_INCLUDE_DIR cmake cache entry to the "
    "${BOOST_INCLUDE_PATH_DESCRIPTION}")

set(BOOST_DIR_SEARCH $ENV{BOOST_ROOT})
if (BOOST_DIR_SEARCH)
  file(TO_CMAKE_PATH ${BOOST_DIR_SEARCH} BOOST_DIR_SEARCH)
  set(BOOST_DIR_SEARCH ${BOOST_DIR_SEARCH}/include)
endif (BOOST_DIR_SEARCH)

if (WIN32)
  set(BOOST_DIR_SEARCH
    ${BOOST_DIR_SEARCH}
    C:/boost/include
    D:/boost/include
  )
endif (WIN32)

# Add in some path suffixes. These will have to be updated whenever a new
# Boost version comes out.
set(SUFFIX_FOR_PATH
 boost-1_35
 boost-1_34_1
 boost-1_34_0
)

#
# Look for an installation.
#
find_path(Boost_INCLUDE_DIR
  NAMES boost/config.hpp
  PATH_SUFFIXES ${SUFFIX_FOR_PATH}
  # Look in other places.
  PATHS ${BOOST_DIR_SEARCH}
  # Help the user find it if we cannot.
  DOC "The ${BOOST_INCLUDE_PATH_DESCRIPTION}"
)

macro(FIND_BOOST_PARENT root includedir)
  # Look for the boost library path.
  # Note that the user may not have installed any libraries
  # so it is quite possible the library path may not exist.
  set(${root} ${includedir})

  if (${${root}} MATCHES "boost-[0-9]+")
    get_filename_component(${root} ${${root}} PATH)
  endif (${${root}} MATCHES "boost-[0-9]+")

  if (${${root}} MATCHES "/include$")
    # Strip off the trailing "/include" in the path.
    get_filename_component(${root} ${${root}} PATH)
  endif (${${root}} MATCHES "/include$")
endmacro(FIND_BOOST_PARENT root includedir)

macro(FIND_BOOST_LIBRARY lib libname libroot)
  set(${lib}_NAMES
    "boost_${libname}-mt"
    "boost_${libname}-gcc42-mt"
    "boost_${libname}-gcc41-mt"
    "boost_${libname}-gcc34-mt"
  )
  if (NOT "${ARGN}" STREQUAL "MT_ONLY")
    set(${lib}_NAMES ${${lib}_NAMES} "boost_${libname}")
  endif (NOT "${ARGN}" STREQUAL "MT_ONLY")

  find_library(${lib}
    NAMES ${${lib}_NAMES}
    PATHS "${libroot}/lib" "${libroot}/lib64"
  )
endmacro(FIND_BOOST_LIBRARY lib libname libroot)

# Assume we didn't find it.
set(Boost_FOUND 0)

# Now try to get the include and library path.
if (Boost_INCLUDE_DIR)

  message(STATUS "Boost include dir: ${Boost_INCLUDE_DIR}")
  FIND_BOOST_PARENT(Boost_PARENT ${Boost_INCLUDE_DIR})

  # Add boost libraries here
  FIND_BOOST_LIBRARY(BOOST_THREAD_LIB thread ${Boost_PARENT})
  FIND_BOOST_LIBRARY(BOOST_PROGRAM_OPTIONS_LIB program_options ${Boost_PARENT})
  FIND_BOOST_LIBRARY(BOOST_IOSTREAMS_LIB iostreams ${Boost_PARENT})

  message(STATUS "Boost thread lib: ${BOOST_THREAD_LIB}")
  message(STATUS "Boost program options lib: ${BOOST_PROGRAM_OPTIONS_LIB}")
  message(STATUS "Boost iostreams lib: ${BOOST_IOSTREAMS_LIB}")
  get_filename_component(Boost_LIBRARY_DIR ${BOOST_THREAD_LIB} PATH)
  message(STATUS "Boost lib dir: ${Boost_LIBRARY_DIR}")

  # BOOST_LIBS is our default boost libs.
  set(BOOST_LIBS ${BOOST_IOSTREAMS_LIB} ${BOOST_PROGRAM_OPTIONS_LIB}
      ${BOOST_THREAD_LIB})

  if (EXISTS "${Boost_INCLUDE_DIR}")
    set(Boost_INCLUDE_DIRS ${Boost_INCLUDE_DIR})
    # We have found boost. It is possible that the user has not
    # compiled any libraries so we set Boost_FOUND to be true here.
    set(Boost_FOUND 1)
  endif (EXISTS "${Boost_INCLUDE_DIR}")

  if (Boost_LIBRARY_DIR AND EXISTS "${Boost_LIBRARY_DIR}")
    set(Boost_LIBRARY_DIRS ${Boost_LIBRARY_DIR})
  endif (Boost_LIBRARY_DIR AND EXISTS "${Boost_LIBRARY_DIR}")

  try_run(BOOST_CHECK SHOULD_COMPILE
          ${HYPERTABLE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeTmp
          ${HYPERTABLE_SOURCE_DIR}/cmake/CheckBoost.cc
          CMAKE_FLAGS -DINCLUDE_DIRECTORIES:STRING=${Boost_INCLUDE_DIRS}
          COMPILE_DEFINITIONS -v
          OUTPUT_VARIABLE BOOST_TRY_OUT)
  string(REGEX REPLACE ".*\n([0-9_]+).*" "\\1" BOOST_VERSION ${BOOST_TRY_OUT})
  string(REGEX REPLACE ".*\ngcc version ([0-9.]+).*" "\\1" GCC_VERSION
         ${BOOST_TRY_OUT})
  message(STATUS "Boost version: ${BOOST_VERSION}")

  if (NOT BOOST_CHECK STREQUAL "0")
    message(FATAL_ERROR "Boost version not compatible")
  endif (NOT BOOST_CHECK STREQUAL "0")

  if (GCC_VERSION)
    message(STATUS "gcc version: ${GCC_VERSION}")
  endif (GCC_VERSION)

endif (Boost_INCLUDE_DIR)

if (NOT Boost_FOUND)
  if (NOT Boost_FIND_QUIETLY)
    message(STATUS "Boost was not found. ${BOOST_DIR_MESSAGE}")
  else (NOT Boost_FIND_QUIETLY)
    if (Boost_FIND_REQUIRED)
      message(FATAL_ERROR "Boost was not found. ${BOOST_DIR_MESSAGE}")
    endif (Boost_FIND_REQUIRED)
  endif (NOT Boost_FIND_QUIETLY)
endif (NOT Boost_FOUND)

mark_as_advanced(
  Boost_INCLUDE_DIR
  BOOST_THREAD_LIB
  BOOST_IOSTREAMS_LIB
  BOOST_PROGRAM_OPTIONS_LIB
)
