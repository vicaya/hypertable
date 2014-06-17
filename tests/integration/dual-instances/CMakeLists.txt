
set(ADDITIONAL_MAKE_CLEAN_FILES ${DST_DIR}/words)

add_test(Dual-instances env INSTALL_DIR=${INSTALL_DIR}
         env TEST_DATA_DIR=${HYPERTABLE_SOURCE_DIR}/tests/data
         ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
