include_directories("../../../src/cc" ${JAVA_INCLUDE_PATH} ${JAVA_INCLUDE_PATH2} ${HADOOP_INCLUDE_PATH})
add_library(MapReduce TableRangeMap.cc TableInputFormat.cc TableReader.cc)
add_dependencies(MapReduce Hypertable)
target_link_libraries(MapReduce Hypertable)
