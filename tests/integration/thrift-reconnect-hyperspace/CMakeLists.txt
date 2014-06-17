add_test(ThriftClient-reconnect-hyperspace env
         THRIFT_CPP_TEST_DIR=${HYPERTABLE_BINARY_DIR}/src/cc/ThriftBroker/ env 
         INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
