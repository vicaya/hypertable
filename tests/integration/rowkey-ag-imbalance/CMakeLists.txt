
set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/tests/integration/rowkey-ag-imbalance")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/tests/integration/rowkey-ag-imbalance")
configure_file(${SRC_DIR}/bar.spec ${DST_DIR}/bar.spec COPYONLY)
configure_file(${SRC_DIR}/foo.spec ${DST_DIR}/foo.spec COPYONLY)

add_test(RangeServer-rowkey-ag-imbalance env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
