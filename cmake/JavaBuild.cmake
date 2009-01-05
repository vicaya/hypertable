if (Thrift_FOUND)
  set(ThriftGenJava_DIR
      ${HYPERTABLE_SOURCE_DIR}/src/gen-java/org/hypertable/thriftgen)

  set(ThriftGenJava_SRCS
      ${ThriftGenJava_DIR}/Cell.java
      ${ThriftGenJava_DIR}/CellFlag.java
      ${ThriftGenJava_DIR}/CellInterval.java
      ${ThriftGenJava_DIR}/ClientException.java
      ${ThriftGenJava_DIR}/ClientService.java
      ${ThriftGenJava_DIR}/HqlResult.java
      ${ThriftGenJava_DIR}/HqlService.java
      ${ThriftGenJava_DIR}/RowInterval.java
      ${ThriftGenJava_DIR}/ScanSpec.java
    )

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
