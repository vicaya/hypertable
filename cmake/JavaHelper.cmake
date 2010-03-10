if (Thrift_FOUND)
  HT_GLOB(ThriftGenJava_SRCS src/gen-java/*.java)

  add_custom_command(
    OUTPUT    ${ThriftGenJava_SRCS}
    COMMAND   thrift
    ARGS      -r -gen java -o ${HYPERTABLE_SOURCE_DIR}/src
              ${ThriftBroker_IDL_DIR}/Hql.thrift
    DEPENDS   ${ThriftBroker_IDL_DIR}/Client.thrift
              ${ThriftBroker_IDL_DIR}/Hql.thrift
    COMMENT   "thrift2java"
  )
endif ()

configure_file(${HYPERTABLE_SOURCE_DIR}/build.xml.in build.xml @ONLY)
add_custom_target(HypertableJavaComponents ALL ant -f build.xml
                  -Dbuild.dir=${HYPERTABLE_BINARY_DIR}/java jar
                  DEPENDS ${ThriftGenJava_SRCS})
add_custom_target(HypertableJavaExamples ALL ant -f build.xml
                  -Dbuild.dir=${HYPERTABLE_BINARY_DIR}/java examples
                  DEPENDS ${ThriftGenJava_SRCS})

add_custom_target(java)
add_dependencies(java HypertableJavaExamples HypertableJavaComponents)

