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

add_subdirectory(rb/HTMonitoring)

if (Thrift_FOUND)
  add_subdirectory(cc/ThriftBroker)
  add_subdirectory(rb/ThriftClient)
  add_subdirectory(perl/ThriftClient)
  add_subdirectory(py/ThriftClient)
  add_subdirectory(php/ThriftClient)
  add_subdirectory(java/ThriftClient)
endif ()

if (Kfs_FOUND)
   add_subdirectory(cc/DfsBroker/kosmos)
endif ()

if (Ceph_FOUND)
   add_subdirectory(cc/DfsBroker/ceph)
endif ()
