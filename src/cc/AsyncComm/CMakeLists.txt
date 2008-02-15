#
# Copyright (C) 2008 Doug Judd (Zvents, Inc.)
# 
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# 

set (SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/AsyncComm/tests")
set (DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/AsyncComm")

set ( TEST_DEPENDENCIES ${DST_DIR}/words )

set (AsyncComm_SRCS
DispatchHandlerSynchronizer.cc
Comm.cc
ConnectionManager.cc
Event.cc
IOHandler.cc
IOHandlerAccept.cc
IOHandlerData.cc
IOHandlerDatagram.cc
HeaderBuilder.cc
Header.cc
Protocol.cc
Reactor.cc
ReactorFactory.cc
ReactorRunner.cc
RequestCache.cc
ResponseCallback.cc
)

add_library (HyperComm ${AsyncComm_SRCS})

# sampleClient
add_executable (sampleClient sampleClient.cc)
target_link_libraries (sampleClient pthread ${BOOST_LIB} HyperComm HyperCommon log4cpp)

# testServer
add_executable (testServer testServer.cc)
target_link_libraries (testServer pthread ${BOOST_LIB} HyperComm HyperCommon log4cpp)

# commTest
add_executable (commTest tests/commTest.cc tests/CommTestThreadFunction.cc ${TEST_DEPENDENCIES})
target_link_libraries (commTest pthread ${BOOST_LIB} HyperComm HyperCommon log4cpp expat)

# commTestDatagram
add_executable (commTestDatagram tests/commTestDatagram.cc tests/CommTestDatagramThreadFunction.cc ${TEST_DEPENDENCIES})
target_link_libraries (commTestDatagram pthread ${BOOST_LIB} HyperComm HyperCommon log4cpp expat)

# commTestTimeout
add_executable (commTestTimeout tests/commTestTimeout.cc)
target_link_libraries (commTestTimeout pthread ${BOOST_LIB} HyperComm HyperCommon log4cpp expat)

# commTestTimer
add_executable (commTestTimer tests/commTestTimer.cc)
target_link_libraries (commTestTimer pthread ${BOOST_LIB} HyperComm HyperCommon log4cpp expat)

# commTestReverseRequest
add_executable (commTestReverseRequest tests/commTestReverseRequest.cc)
target_link_libraries (commTestReverseRequest pthread ${BOOST_LIB} HyperComm HyperCommon log4cpp expat)

configure_file(${SRC_DIR}/commTestTimeout.golden ${DST_DIR}/commTestTimeout.golden)
configure_file(${SRC_DIR}/commTestTimer.golden ${DST_DIR}/commTestTimer.golden)
configure_file(${SRC_DIR}/datafile.txt ${DST_DIR}/datafile.txt)
configure_file(${SRC_DIR}/commTestReverseRequest.golden ${DST_DIR}/commTestReverseRequest.golden)

add_custom_command (SOURCE ${HYPERTABLE_SOURCE_DIR}/test/words.gz
                    COMMAND cp ARGS ${HYPERTABLE_SOURCE_DIR}/test/words.gz ${DST_DIR}
                    COMMAND gunzip ARGS ${DST_DIR}/words.gz
                    OUTPUT ${DST_DIR}/words)

set (ADDITIONAL_MAKE_CLEAN_FILES ${DST_DIR}/words)

add_test (HyperComm commTest)
add_test (HyperComm-datagram commTestDatagram)
add_test (HyperComm-timeout commTestTimeout)
add_test (HyperComm-timer commTestTimer)
add_test (HyperComm-reverse-request commTestReverseRequest)

file (GLOB HEADERS *.h)

install (FILES ${HEADERS} DESTINATION include/hypertable-${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_MICRO}/AsyncComm)

install (TARGETS HyperComm
         RUNTIME DESTINATION bin
         LIBRARY DESTINATION lib
         ARCHIVE DESTINATION lib)