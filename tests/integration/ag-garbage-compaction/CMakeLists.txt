add_test(AG-garbage-collection-max-versions env INSTALL_DIR=${INSTALL_DIR}
         bash ${CMAKE_CURRENT_SOURCE_DIR}/run-max-versions.sh)

add_test(AG-garbage-collection-deletes env INSTALL_DIR=${INSTALL_DIR}
         bash ${CMAKE_CURRENT_SOURCE_DIR}/run-deletes.sh)

add_test(AG-garbage-collection-ttl env INSTALL_DIR=${INSTALL_DIR}
         bash ${CMAKE_CURRENT_SOURCE_DIR}/run-ttl.sh)
