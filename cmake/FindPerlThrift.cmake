# - Find PYTHON5
# This module defines
#  PERLTHRIFT_FOUND, If false, do not try to use ant

exec_program(env ARGS perl -MThrift -e 0 OUTPUT_VARIABLE PERLTHRIFT_OUT
             RETURN_VALUE PERLTHRIFT_RETURN)

if (PERLTHRIFT_RETURN STREQUAL "0")
  set(PERLTHRIFT_FOUND TRUE)
else ()
  set(PERLTHRIFT_FOUND FALSE)
endif ()

if (PERLTHRIFT_FOUND)
  if (NOT PERLTHRIFT_FIND_QUIETLY)
    message(STATUS "Found thrift for perl")
  endif ()
else ()
  message(STATUS "Thrift for perl not found. "
                 "ThriftBroker support for perl will be disabled")
endif ()

