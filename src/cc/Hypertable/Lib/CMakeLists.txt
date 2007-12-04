#
# Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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

set ( Hypertable_SRCS
Client.cc
EventHandlerMasterChange.cc
HqlCommandInterpreter.cc
HqlHelpText.cc
Key.cc
LoadDataSource.cc
LocationCache.cc
MasterClient.cc
MasterFileHandler.cc
MasterProtocol.cc
Metadata.cc
Mutator.cc
MutatorDispatchHandler.cc
MutatorScatterBuffer.cc
RangeLocator.cc
RangeServerClient.cc
RangeServerProtocol.cc
RootFileHandler.cc
ScanBlock.cc
Schema.cc
Table.cc
TableScanner.cc
TestSource.cc
Types.cc
)

add_library (Hypertable ${Hypertable_SRCS})

# generate_test_data
add_executable (generate_test_data generate_test_data.cc)
target_link_libraries (generate_test_data Hypertable Hyperspace pthread ${BOOST_LIB} AsyncComm Common log4cpp expat)

# MetadataTest
add_executable (MetadataTest MetadataTest.cc)
target_link_libraries (MetadataTest pthread ${BOOST_LIB} Hypertable AsyncComm Common log4cpp expat)

# schemaTest
add_executable (schemaTest tests/schemaTest.cc)
target_link_libraries (schemaTest Hypertable pthread ${BOOST_LIB} Common log4cpp expat)

# locationCacheTest
add_executable (locationCacheTest tests/locationCacheTest.cc)
target_link_libraries (locationCacheTest Hypertable ${BOOST_LIB} Common log4cpp)

# loadDataSourceTest
add_executable (loadDataSourceTest tests/loadDataSourceTest.cc)
target_link_libraries (loadDataSourceTest Hypertable ${BOOST_LIB} Common log4cpp)


#
# Copy test files
#
set (SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Hypertable/Lib/tests")
set (DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Hypertable/Lib")
configure_file(${SRC_DIR}/bad-schema-1.xml ${DST_DIR}/bad-schema-1.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-2.xml ${DST_DIR}/bad-schema-2.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-3.xml ${DST_DIR}/bad-schema-3.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-4.xml ${DST_DIR}/bad-schema-4.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-5.xml ${DST_DIR}/bad-schema-5.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-6.xml ${DST_DIR}/bad-schema-6.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-7.xml ${DST_DIR}/bad-schema-7.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-8.xml ${DST_DIR}/bad-schema-8.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-9.xml ${DST_DIR}/bad-schema-9.xml COPYONLY)
configure_file(${SRC_DIR}/good-schema-1.xml ${DST_DIR}/good-schema-1.xml COPYONLY)

configure_file(${HYPERTABLE_SOURCE_DIR}/test/random.dat ${DST_DIR}/random.dat COPYONLY)
configure_file(${SRC_DIR}/schemaTest.golden ${DST_DIR}/schemaTest.golden)
configure_file(${SRC_DIR}/locationCacheTest.golden ${DST_DIR}/locationCacheTest.golden)
configure_file(${SRC_DIR}/loadDataSourceTest.golden ${DST_DIR}/loadDataSourceTest.golden)
configure_file(${SRC_DIR}/loadDataSourceTest.dat ${DST_DIR}/loadDataSourceTest.dat)

add_test (Schema schemaTest)
add_test (LocationCache locationCacheTest)
add_test (LoadDataSource loadDataSourceTest)

install (TARGETS generate_test_data RUNTIME DESTINATION bin)
