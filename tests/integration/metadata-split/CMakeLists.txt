set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/tests/integration/metadata-split")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/tests/integration/metadata-split")

add_test(METADATA-split-recovery-0 env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/orig-run.sh)

add_test(METADATA-split-recovery-1 env TEST_1=1 INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-recovery-2 env TEST_2=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-recovery-3 env TEST_3=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-recovery-4 env TEST_4=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-recovery-5 env TEST_5=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-recovery-6 env TEST_6=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-recovery-7 env TEST_7=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-recovery-8 env TEST_8=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-recovery-9 env TEST_9=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-recovery-10 env TEST_10=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-recovery-11 env TEST_11=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-recovery-12 env TEST_12=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(METADATA-split-recovery-13 env TEST_13=1 SKIP_START_SERVERS=1 
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
