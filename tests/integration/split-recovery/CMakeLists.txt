add_test(RangeServer-split-recovery-1 env TEST_1=1 DATA_SIZE=200000
         RANGE_SIZE=1M INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-split-recovery-2 env TEST_2=1 DATA_SIZE=200000
         RANGE_SIZE=1M SKIP_START_SERVERS=1 INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-split-recovery-3 env TEST_3=1 DATA_SIZE=200000
         RANGE_SIZE=1M SKIP_START_SERVERS=1 INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-split-recovery-4 env TEST_4=1 DATA_SIZE=200000
         RANGE_SIZE=1M SKIP_START_SERVERS=1 INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-split-recovery-5 env TEST_5=1 DATA_SIZE=200000
         RANGE_SIZE=1M SKIP_START_SERVERS=1 INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-split-recovery-6 env TEST_6=1 DATA_SIZE=200000
         RANGE_SIZE=1M SKIP_START_SERVERS=1 INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-split-recovery-7 env TEST_7=1 DATA_SIZE=200000
         RANGE_SIZE=1M SKIP_START_SERVERS=1 INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-split-recovery-8 env TEST_8=1 DATA_SIZE=200000
         RANGE_SIZE=1M SKIP_START_SERVERS=1 INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
