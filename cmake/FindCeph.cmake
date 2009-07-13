# - Find Ceph
# Find the native Ceph includes and library
#
#  Ceph_INCLUDE_DIR - where to find libceph.h, etc.
#  Ceph_LIBRARIES   - List of libraries when using Ceph.
#  Ceph_FOUND       - True if Ceph found.


if (Ceph_INCLUDE)
  # Already in cache, be silent
  set(Ceph_FIND_QUIETLY TRUE)
endif ()

find_path(Ceph_INCLUDE libceph.h
  /usr/local/include
  /usr/include
  $ENV{HOME}/ceph/src/client
)
mark_as_advanced(Ceph_INCLUDE)

find_library(Ceph_LIB
	NAMES ceph
	PATHS /usr/local/lib
	      $ENV{HOME}/ceph/src/.libs)
mark_as_advanced(Ceph_LIB)

if (Ceph_INCLUDE AND Ceph_LIB)
  set(Ceph_FOUND TRUE)
  set(Ceph_LIBRARIES ${Ceph_LIB})
else ()
   set(Ceph_FOUND FALSE)
   set(Ceph_LIBRARIES)
endif ()

if (Ceph_FOUND)
   message(STATUS "Found ceph: ${Ceph_LIBRARIES}")
else ()
   message(STATUS "Did not find ceph libraries")
   if (Ceph_FIND_REQUIRED)
      message(FATAL_ERROR "Could NOT find ceph libraries")
   endif ()
endif ()

