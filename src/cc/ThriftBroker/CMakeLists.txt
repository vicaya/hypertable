#
# Copyright(C) 2008 Doug Judd (Zvents, Inc.)
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#

HT_GLOB(ThriftGen_SRCS gen-cpp/*Service.cpp gen-cpp/*constants.cpp
        gen-cpp/*types.cpp)

add_library(HyperThrift ThriftHelper.cc ${ThriftGen_SRCS})
target_link_libraries(HyperThrift ${Thrift_LIBS} ${LibEvent_LIBS})

add_library(HyperThriftConfig Config.cc)
target_link_libraries(HyperThriftConfig HyperThrift Hypertable)

set(ThriftBroker_SRCS ThriftBroker.cc)

add_custom_command(
  OUTPUT    ${ThriftGen_SRCS}
  COMMAND   thrift
  ARGS      -r --gen cpp -o ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/Hql.thrift
  DEPENDS   ${CMAKE_CURRENT_SOURCE_DIR}/Client.thrift
            ${CMAKE_CURRENT_SOURCE_DIR}/Hql.thrift
  COMMENT   "thrift2cpp"
)

# ThriftBroker
add_executable(ThriftBroker ${ThriftBroker_SRCS})
target_link_libraries(ThriftBroker HyperThriftConfig ${MALLOC_LIBRARY})

# Basic regression test
add_executable(client_test tests/client_test.cc)
target_link_libraries(client_test HyperThrift HyperCommon)
add_test(ThriftClient-cpp client_test)

if (NOT HT_COMPONENT_INSTALL OR PACKAGE_THRIFTBROKER)
  install(TARGETS HyperThrift HyperThriftConfig ThriftBroker
          RUNTIME DESTINATION bin
          LIBRARY DESTINATION lib
          ARCHIVE DESTINATION lib)
  install(FILES Client.h ThriftHelper.h Client.thrift Hql.thrift
          DESTINATION include/ThriftBroker)
endif ()

if (PACKAGE_THRIFTBROKER)
  file(GLOB SCRIPTS ${CMAKE_SOURCE_DIR}/bin/*thriftbroker.sh)
  install(PROGRAMS ${SCRIPTS} DESTINATION bin)
  set(CPACK_PACKAGE_NAME "hypertable-thriftbroker")
  include(PackageHelper)
endif ()
