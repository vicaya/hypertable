if (Thrift_FOUND)
  add_custom_target(thriftdoc thrift -gen html -r
                    ${ThriftBroker_IDL_DIR}/Hql.thrift)
endif ()

add_custom_target(hqldoc ${CMAKE_SOURCE_DIR}/doc/bin/make-doc-tree.sh hqldoc)

if (DOXYGEN_FOUND)
  configure_file(${HYPERTABLE_SOURCE_DIR}/doc/Doxyfile
                 ${HYPERTABLE_BINARY_DIR}/Doxyfile)
  add_custom_target(doc ${DOXYGEN_EXECUTABLE} ${HYPERTABLE_BINARY_DIR}/Doxyfile)

  add_custom_command(TARGET doc PRE_BUILD COMMAND ${CMAKE_COMMAND} -E copy_directory 
                     ${CMAKE_SOURCE_DIR}/doc/markdown markdown POST_BUILD COMMAND make hqldoc)

  if (Thrift_FOUND)
    add_custom_command(TARGET doc POST_BUILD COMMAND make thriftdoc)
  endif ()

endif ()

