add_test(RangeServer-sequential-load env INSTALL_DIR=${INSTALL_DIR}
         env SOURCE_DIR=${HYPERTABLE_SOURCE_DIR}/tests/integration/sequential-load
         ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
