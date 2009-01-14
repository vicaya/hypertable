if (Thrift_FOUND)
  add_custom_target(thriftdoc thrift -gen html -r
                    ${ThriftBroker_IDL_DIR}/Hql.thrift)
endif ()

if (DOXYGEN_FOUND)
  configure_file(${HYPERTABLE_SOURCE_DIR}/doc/Doxyfile
                 ${HYPERTABLE_BINARY_DIR}/Doxyfile)
  add_custom_target(doc ${DOXYGEN_EXECUTABLE} ${HYPERTABLE_BINARY_DIR}/Doxyfile)
  add_custom_command(TARGET doc POST_BUILD COMMAND make thriftdoc)
endif ()

