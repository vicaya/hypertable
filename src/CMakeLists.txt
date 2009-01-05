# CMake file for hypertable source

add_subdirectory(cc/Common)
add_subdirectory(cc/AsyncComm)
add_subdirectory(cc/Hyperspace)
add_subdirectory(cc/Hypertable/Master)
add_subdirectory(cc/Hypertable/RangeServer)
add_subdirectory(cc/Hypertable/Lib)
add_subdirectory(cc/Tools)
add_subdirectory(cc/DfsBroker/Lib)
add_subdirectory(cc/DfsBroker/local)
add_subdirectory(cc/Benchmark)

if (Thrift_FOUND)
  add_subdirectory(cc/ThriftBroker)

  if (RUBYTHRIFT_FOUND)
    add_subdirectory(rb/ThriftClient)
  endif ()

  if (PERLTHRIFT_FOUND)
    add_subdirectory(perl/ThriftClient)
  endif ()

  if (PYTHONTHRIFT_FOUND)
    add_subdirectory(py/ThriftClient)
  endif ()

  if (PHPTHRIFT_FOUND)
    add_subdirectory(php/ThriftClient)
  endif ()

  if (JAVATHRIFT_FOUND)
    add_subdirectory(java/ThriftClient)
  endif ()
endif ()

if (Kfs_FOUND)
   add_subdirectory(cc/DfsBroker/kosmos)
endif ()
