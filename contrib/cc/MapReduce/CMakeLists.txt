include_directories("../../../src/cc" ${JAVA_INCLUDE_PATH} ${JAVA_INCLUDE_PATH2} ${HADOOP_INCLUDE_PATH})
link_directories(${HADOOP_LIB_PATH})

add_library(MapReduce TableRangeMap.cc TableInputFormat.cc TableReader.cc)
add_dependencies(MapReduce Hypertable)
target_link_libraries(MapReduce Hypertable)

# examples/tests
add_executable(mapredjob test/mapredjob.cc)
target_link_libraries(mapredjob MapReduce hadooppipes hadooputils)

