add_test(RangeServer-bloomfilter-rows env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run.sh rows)
add_test(RangeServer-bloomfilter-rows-cols env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run.sh rows-cols)
