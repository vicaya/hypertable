add_test(Hypertable-multiple-maintenance-threads env
         INSTALL_DIR=${CMAKE_INSTALL_PREFIX}/${VERSION}
         ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
