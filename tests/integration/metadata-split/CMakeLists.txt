set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/tests/integration/metadata-split")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/tests/integration/metadata-split")
configure_file(${SRC_DIR}/data.spec ${DST_DIR}/data.spec COPYONLY)

add_test(RangeServer-metadata-split env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
