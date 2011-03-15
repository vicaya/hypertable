add_test(Client-future-abrupt-end env INSTALL_DIR=${INSTALL_DIR}
         TEST_BIN_DIR=${HYPERTABLE_BINARY_DIR}/src/cc/Hypertable/Lib/
         ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
