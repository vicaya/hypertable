include_directories(${JAVA_INCLUDE_PATH} ${JAVA_INCLUDE_PATH2}
                    ${HADOOP_INCLUDE_PATH})
link_directories(${HADOOP_LIB_PATH})

add_library(MapReduce TableReader.cc)
add_dependencies(MapReduce Hypertable)
target_link_libraries(MapReduce hadooppipes hadooputils Hypertable)

set(BUILD_SHARED_LIBS ON)
add_library(MapReduceJNI TableInputFormat.cc TableRangeMap.cc)
target_link_libraries(MapReduceJNI hadooppipes hadooputils Hypertable)

# examples/tests
add_executable(mapredjob test/mapredjob.cc)
target_link_libraries(mapredjob hadooppipes hadooputils MapReduce)

add_executable(range_map_test test/rangeMapTest.cc)
target_link_libraries(range_map_test MapReduceJNI)

install(TARGETS MapReduce
    LIBRARY DESTINATION ${VERSION}/lib
    ARCHIVE DESTINATION ${VERSION}/lib)

file(GLOB HEADERS *.h)
install(FILES ${HEADERS} DESTINATION ${VERSION}/include/Hypertable/Mapreduce)

