add_test(Client-no-log-sync env
         TEST_BIN_DIR=${HYPERTABLE_BINARY_DIR}/src/cc/Hypertable/Lib/
         ${CMAKE_CURRENT_SOURCE_DIR}/run.sh ${INSTALL_DIR}/bin)
