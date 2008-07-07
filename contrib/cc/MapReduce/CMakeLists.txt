include_directories("../../../src/cc" ${JAVA_INCLUDE_PATH} ${JAVA_INCLUDE_PATH2})
add_library(MapReduce TableRangeMap.cc TableInputFormat.cc)
add_dependencies(MapReduce Hypertable)
target_link_libraries(MapReduce Hypertable)
