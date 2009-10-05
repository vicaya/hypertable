# - Find PYTHON5
# This module defines
#  RUBYTHRIFT_FOUND, If false, do not try to use ant

exec_program(env ARGS ruby -r thrift -e 0 OUTPUT_VARIABLE RUBYTHRIFT_OUT
             RETURN_VALUE RUBYTHRIFT_RETURN)

if (RUBYTHRIFT_RETURN STREQUAL "0")
  set(RUBYTHRIFT_FOUND TRUE)
else ()
  set(RUBYTHRIFT_FOUND FALSE)
endif ()

if (RUBYTHRIFT_FOUND)
  if (NOT RUBYTHRIFT_FIND_QUIETLY)
    message(STATUS "Found thrift for ruby")
  endif ()
else ()
  message(STATUS "Thrift for ruby not found. "
                 "ThriftBroker support for ruby will be disabled")
endif ()

