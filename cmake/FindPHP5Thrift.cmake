# - Find PHP5
# This module defines
#  PHPTHRIFT_FOUND, If false, do not try to use ant

if (PHPTHRIFT_ROOT)
  if (EXISTS ${PHPTHRIFT_ROOT}/Thrift.php)
    set(PHPTHRIFT_FOUND TRUE)
    set(PHPTHRIFT_ROOT ${PHPTHRIFT_ROOT} CACHE PATH "php thrift root dir" FORCE)
  endif ()
else ()
  set(PHPTHRIFT_FOUND FALSE)
endif ()

if (PHPTHRIFT_FOUND)
  if (NOT PHPTHRIFT_FIND_QUIETLY)
    message(STATUS "Found thrift for php: ${PHPTHRIFT_ROOT}")
  endif ()
else ()
  message(STATUS "PHPTHRIFT_ROOT not found. "
                 "ThriftBroker support for php will be disabled")
endif ()

mark_as_advanced(
  PHPTHRIFT_ROOT
)

