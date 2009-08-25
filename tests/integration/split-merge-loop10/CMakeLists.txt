add_test(RangeServer-split-merge-loop10 env INSTALL_DIR=${INSTALL_DIR}
         env SOURCE_DIR=${HYPERTABLE_SOURCE_DIR}/src/cc/Tools/rsclient/tests
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
