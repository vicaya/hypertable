set(INSTALLED_SERVERS
  ${INSTALL_DIR}/bin/Hyperspace.Master
  ${INSTALL_DIR}/bin/Hypertable.Master
  ${INSTALL_DIR}/bin/Hypertable.RangeServer
  ${INSTALL_DIR}/bin/localBroker
)

if (Thrift_FOUND)
  set(INSTALLED_SERVERS ${INSTALLED_SERVERS} ${INSTALL_DIR}/bin/ThriftBroker)
endif ()

if (Kfs_FOUND)
  set(INSTALLED_SERVERS ${INSTALLED_SERVERS} ${INSTALL_DIR}/bin/kosmosBroker)
endif ()

if (Ceph_FOUND)
  set(INSTALLED_SERVERS ${INSTALLED_SERVERS} ${INSTALL_DIR}/bin/cephBroker)
endif ()

set(TEST_SERVERS_STARTED ${HYPERTABLE_BINARY_DIR}/test-servers-started)

add_custom_command(
  OUTPUT    ${TEST_SERVERS_STARTED}
  COMMAND   ${INSTALL_DIR}/bin/start-test-servers.sh
  ARGS      --clear
  DEPENDS   ${INSTALLED_SERVERS}
  COMMENT   "Starting test servers"
)

add_custom_target(runtestservers DEPENDS ${TEST_SERVERS_STARTED})

macro(add_test_target target dir)
  add_custom_target(${target})
  add_dependencies(${target} runtestservers)
  add_custom_command(TARGET ${target} POST_BUILD COMMAND ${INSTALL_DIR}/bin/ht $(MAKE) test
                     WORKING_DIRECTORY ${dir})
endmacro()

# custom target must be globally unique to support IDEs like Xcode, VS etc.
add_test_target(alltests ${HYPERTABLE_BINARY_DIR})
add_test_target(coretests ${HYPERTABLE_BINARY_DIR}/src)
add_test_target(moretests ${HYPERTABLE_BINARY_DIR}/tests/integration)
