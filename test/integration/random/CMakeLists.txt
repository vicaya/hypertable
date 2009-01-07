add_test(Hypertable-random-write-read env
         INSTALL_DIR=${CMAKE_INSTALL_PREFIX}/${VERSION}
         ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
