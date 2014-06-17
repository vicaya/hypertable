add_test(RangeServer-metadata-update-recovery env DATA_SIZE=300
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
