# - Find PYTHON5
# This module defines
#  PYTHONTHRIFT_FOUND, If false, do not try to use ant

exec_program(env ARGS python -c'import thrift' OUTPUT_VARIABLE PYTHONTHRIFT_OUT
             RETURN_VALUE PYTHONTHRIFT_RETURN)

if (PYTHONTHRIFT_RETURN STREQUAL "0")
  set(PYTHONTHRIFT_FOUND TRUE)
else ()
  set(PYTHONTHRIFT_FOUND FALSE)
endif ()

if (PYTHONTHRIFT_FOUND)
  if (NOT PYTHONTHRIFT_FIND_QUIETLY)
    message(STATUS "Found thrift for python")
  endif ()
else ()
  message(STATUS "Thrift for python not found. "
                 "ThriftBroker support for python will be disabled")
endif ()

