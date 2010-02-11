set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/tests/integration/metadata-split")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/tests/integration/metadata-split")

add_test(METADATA-split-0 env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/orig-run.sh)

add_test(METADATA-split-1 env TEST_1=1 INSTALL_DIR=${INSTALL_DIR}
         sh -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-2 env TEST_2=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         sh -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-3 env TEST_3=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         sh -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-4 env TEST_4=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         sh -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-5 env TEST_5=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         sh -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-6 env TEST_6=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         sh -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-7 env TEST_7=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         sh -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-8 env TEST_8=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         sh -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-9 env TEST_9=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         sh -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
