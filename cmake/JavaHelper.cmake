if (Thrift_FOUND)
  set(ThriftGenJava_DIR
      ${HYPERTABLE_SOURCE_DIR}/src/gen-java/org/hypertable/thriftgen)

  file(GLOB ThriftGenJava_SRCS ${ThriftGenJava_DIR}/*.java)

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
