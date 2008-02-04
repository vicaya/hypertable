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

set ( Hypertable_SRCS
ApacheLogParser.cc
BlockCompressionCodecBmz.cc
BlockCompressionCodecLzo.cc
BlockCompressionCodecNone.cc
BlockCompressionCodecQuicklz.cc
BlockCompressionCodecZlib.cc
BlockCompressionHeaderCommitLog.cc
CompressorFactory.cc
Client.cc
CommitLog.cc
CommitLogReader.cc
Defaults.cc
EventHandlerMasterChange.cc
fletcher16.cc
Filesystem.cc
HqlCommandInterpreter.cc
HqlHelpText.cc
Key.cc
LoadDataSource.cc
LocationCache.cc
MasterClient.cc
MasterFileHandler.cc
MasterProtocol.cc
RangeLocator.cc
RangeServerClient.cc
RangeServerProtocol.cc
RootFileHandler.cc
ScanBlock.cc
Schema.cc
Table.cc
TableMutator.cc
TableMutatorDispatchHandler.cc
TableMutatorScatterBuffer.cc
TableScanner.cc
TestSource.cc
Types.cc
lzo/minilzo.c
bmz/bmz.c
)

add_library (Hypertable ${Hypertable_SRCS})
add_dependencies (Hypertable Hyperspace HyperComm HyperCommon ${BOOST_LIB})
target_link_libraries (Hypertable Hyperspace HyperComm HyperCommon ${BOOST_LIB})

# generate_test_data
add_executable (generate_test_data generate_test_data.cc)
target_link_libraries (generate_test_data Hypertable pthread ${BOOST_LIB} log4cpp expat)

# schemaTest
add_executable (schemaTest tests/schemaTest.cc)
target_link_libraries (schemaTest Hypertable pthread ${BOOST_LIB} log4cpp expat)

# locationCacheTest
add_executable (locationCacheTest tests/locationCacheTest.cc)
target_link_libraries (locationCacheTest Hypertable ${BOOST_LIB} log4cpp)

# loadDataSourceTest
add_executable (loadDataSourceTest tests/loadDataSourceTest.cc)
target_link_libraries (loadDataSourceTest Hypertable ${BOOST_LIB} log4cpp)

# compressor_test
add_executable (compressor_test tests/compressor_test.cc)
target_link_libraries (compressor_test Hypertable log4cpp ${ZLIB_LIBRARIES} ${Lzo2_LIBRARIES})

# bmz binaries
add_executable(bmz-test bmz/bmz-test.c)
target_link_libraries(bmz-test Hypertable log4cpp)

add_executable(bmzip bmz/bmzip.c)
target_link_libraries(bmzip Hypertable log4cpp)

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
add_test (BlockCompressor-BMZ compressor_test bmz)
add_test (BlockCompressor-LZO compressor_test lzo)
add_test (BlockCompressor-NONE compressor_test none)
add_test (BlockCompressor-QUICKLZ compressor_test quicklz)
add_test (BlockCompressor-ZLIB compressor_test zlib)

file (GLOB HEADERS *.h)

install (FILES ${HEADERS} DESTINATION include/hypertable-${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_MICRO}/Hypertable/Lib)

install (TARGETS generate_test_data RUNTIME DESTINATION bin)

install (TARGETS Hypertable
         RUNTIME DESTINATION bin
         LIBRARY DESTINATION lib
         ARCHIVE DESTINATION lib)
