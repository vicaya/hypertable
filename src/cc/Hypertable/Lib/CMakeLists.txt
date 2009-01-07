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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#

set(Hypertable_SRCS
ApacheLogParser.cc
BlockCompressionCodec.cc
BlockCompressionCodecBmz.cc
BlockCompressionCodecLzo.cc
BlockCompressionCodecNone.cc
BlockCompressionCodecQuicklz.cc
BlockCompressionCodecZlib.cc
BlockCompressionHeader.cc
BlockCompressionHeaderCommitLog.cc
Cell.cc
CompressorFactory.cc
Client.cc
CommitLogBlockStream.cc
CommitLog.cc
CommitLogReader.cc
Config.cc
Defaults.cc
EventHandlerMasterChange.cc
Filesystem.cc
FixedRandomStringGenerator.cc
HqlInterpreter.cc
HqlCommandInterpreter.cc
HqlHelpText.cc
IntervalScanner.cc
Key.cc
KeySpec.cc
LoadDataEscape.cc
LoadDataSource.cc
LocationCache.cc
MasterClient.cc
MasterFileHandler.cc
MasterProtocol.cc
MetaLog.cc
MetaLogReader.cc
MetaLogVersion.cc
MetaLogDfsBase.cc
MetaLogReaderDfsBase.cc
RangeServerMetaLogEntryFactory.cc
RangeServerMetaLog.cc
RangeServerMetaLogReader.cc
MetaLogEntryBase.cc
RangeServerMetaLogEntries.cc
MasterMetaLogEntryFactory.cc
MasterMetaLog.cc
MasterMetaLogReader.cc
RangeLocator.cc
RangeServerClient.cc
RangeServerProtocol.cc
RangeState.cc
RootFileHandler.cc
ScanBlock.cc
ScanSpec.cc
Schema.cc
Stat.cc
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

add_library(Hypertable ${Hypertable_SRCS})
add_dependencies(Hypertable HyperComm Hyperspace HyperCommon)
target_link_libraries(Hypertable ${EXPAT_LIBRARIES} Hyperspace)

# generate_test_data
add_executable(generate_test_data generate_test_data.cc)
target_link_libraries(generate_test_data Hypertable)

# meta log test
add_executable(metalog_api_test tests/MetaLogApiTest.cc)
target_link_libraries(metalog_api_test HyperDfsBroker)
add_executable(metalog_master_test tests/MetaLogMasterTest.cc)
target_link_libraries(metalog_master_test HyperDfsBroker)
add_executable(metalog_rs_test tests/MetaLogRangeServerTest.cc)
target_link_libraries(metalog_rs_test HyperDfsBroker)

# meta log dumper
add_executable(dump_metalog tests/DumpMetaLog.cc)
target_link_libraries(dump_metalog HyperDfsBroker)

# schemaTest
add_executable(schemaTest tests/schemaTest.cc)
target_link_libraries(schemaTest Hypertable)

# locationCacheTest
add_executable(locationCacheTest tests/locationCacheTest.cc)
target_link_libraries(locationCacheTest Hypertable)

# loadDataSourceTest
add_executable(loadDataSourceTest tests/loadDataSourceTest.cc)
target_link_libraries(loadDataSourceTest Hypertable)

# compressor_test
add_executable(compressor_test tests/compressor_test.cc)
target_link_libraries(compressor_test Hypertable)

# bmz binaries
add_executable(bmz-test bmz/bmz-test.c)
target_link_libraries(bmz-test Hypertable)

add_executable(bmzip bmz/bmzip.c)
target_link_libraries(bmzip Hypertable)

# commit_log_test
add_executable(commit_log_test tests/commit_log_test.cc)
target_link_libraries(commit_log_test HyperDfsBroker)

# escape_test
add_executable(escape_test tests/escape_test.cc)
target_link_libraries(escape_test Hypertable)

# large_insert_test
add_executable(large_insert_test tests/large_insert_test.cc)
target_link_libraries(large_insert_test Hypertable)

#
# Copy test files
#
set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Hypertable/Lib/tests")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Hypertable/Lib")
configure_file(${SRC_DIR}/bad-schema-1.xml ${DST_DIR}/bad-schema-1.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-2.xml ${DST_DIR}/bad-schema-2.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-3.xml ${DST_DIR}/bad-schema-3.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-4.xml ${DST_DIR}/bad-schema-4.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-5.xml ${DST_DIR}/bad-schema-5.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-6.xml ${DST_DIR}/bad-schema-6.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-7.xml ${DST_DIR}/bad-schema-7.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-8.xml ${DST_DIR}/bad-schema-8.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-9.xml ${DST_DIR}/bad-schema-9.xml COPYONLY)
configure_file(${SRC_DIR}/good-schema-1.xml ${DST_DIR}/good-schema-1.xml
               COPYONLY)

configure_file(${HYPERTABLE_SOURCE_DIR}/test/data/random.dat
               ${DST_DIR}/random.dat COPYONLY)
configure_file(${SRC_DIR}/schemaTest.golden ${DST_DIR}/schemaTest.golden)
configure_file(${SRC_DIR}/locationCacheTest.golden
               ${DST_DIR}/locationCacheTest.golden)
configure_file(${SRC_DIR}/loadDataSourceTest.golden
               ${DST_DIR}/loadDataSourceTest.golden)
configure_file(${SRC_DIR}/loadDataSourceTest.dat
               ${DST_DIR}/loadDataSourceTest.dat)
configure_file(${SRC_DIR}/loadDataSourceTest-header.golden
               ${DST_DIR}/loadDataSourceTest-header.golden)
configure_file(${SRC_DIR}/loadDataSourceTest-header.dat
               ${DST_DIR}/loadDataSourceTest-header.dat)
configure_file(${SRC_DIR}/loadDataSourceTest-qualified-header.golden
               ${DST_DIR}/loadDataSourceTest-qualified-header.golden)
configure_file(${SRC_DIR}/loadDataSourceTest-qualified-header.dat
               ${DST_DIR}/loadDataSourceTest-qualified-header.dat)
configure_file(${HYPERTABLE_SOURCE_DIR}/conf/hypertable.cfg
               ${DST_DIR}/hypertable.cfg)

add_test(Schema schemaTest)
add_test(LocationCache locationCacheTest)
add_test(LoadDataSource loadDataSourceTest)
add_test(LoadDataEscape escape_test)
add_test(BlockCompressor-BMZ compressor_test bmz)
add_test(BlockCompressor-LZO compressor_test lzo)
add_test(BlockCompressor-NONE compressor_test none)
add_test(BlockCompressor-QUICKLZ compressor_test quicklz)
add_test(BlockCompressor-ZLIB compressor_test zlib)
add_test(CommitLog commit_log_test)
add_test(MetaLog-Master metalog_master_test)
add_test(MetaLog-RangeServer metalog_rs_test)
add_test(Client-large-block large_insert_test)

file(GLOB HEADERS *.h)

install(FILES ${HEADERS} DESTINATION ${VERSION}/include/Hypertable/Lib)

install(TARGETS generate_test_data RUNTIME DESTINATION ${VERSION}/bin)

install(TARGETS Hypertable dump_metalog
        RUNTIME DESTINATION ${VERSION}/bin
        LIBRARY DESTINATION ${VERSION}/lib
        ARCHIVE DESTINATION ${VERSION}/lib)
