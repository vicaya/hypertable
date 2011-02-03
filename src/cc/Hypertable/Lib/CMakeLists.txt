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
Client.cc
CommitLog.cc
CommitLogBlockStream.cc
CommitLogReader.cc
CompressorFactory.cc
Config.cc
DataGenerator.cc
EventHandlerMasterChange.cc
FixedRandomStringGenerator.cc
HqlCommandInterpreter.cc
HqlHelpText.cc
HqlInterpreter.cc
IntervalScanner.cc
Key.cc
KeySpec.cc
LoadDataEscape.cc
LoadDataSource.cc
LoadDataSourceFactory.cc
LoadDataSourceFileDfs.cc
LoadDataSourceFileLocal.cc
LoadDataSourceStdin.cc
LocationCache.cc
MasterClient.cc
MasterFileHandler.cc
MasterProtocol.cc
MetaLog.cc
MetaLogEntity.cc
MetaLogEntityHeader.cc
MetaLogReader.cc
MetaLogWriter.cc
NameIdMapper.cc
Namespace.cc
NamespaceCache.cc
RangeLocator.cc
RangeServerClient.cc
RangeServerProtocol.cc
RangeState.cc
RootFileHandler.cc
ScanBlock.cc
ScanSpec.cc
Schema.cc
StatsRangeServer.cc
StatsTable.cc
Table.cc
TableCache.cc
TableDumper.cc
TableMutator.cc
TableMutatorDispatchHandler.cc
TableMutatorFlushHandler.cc
TableMutatorIntervalHandler.cc
TableMutatorScatterBuffer.cc
TableMutatorShared.cc
TableMutatorSyncDispatchHandler.cc
TableScanner.cc
TableSplit.cc
TestSource.cc
Types.cc
old/MasterMetaLog.cc
old/MasterMetaLogEntries.cc
old/MasterMetaLogEntryFactory.cc
old/MasterMetaLogReader.cc
old/MetaLog.cc
old/MetaLogDfsBase.cc
old/MetaLogEntryBase.cc
old/MetaLogReader.cc
old/MetaLogReaderDfsBase.cc
old/MetaLogVersion.cc
old/RangeServerMetaLog.cc
old/RangeServerMetaLogEntries.cc
old/RangeServerMetaLogEntryFactory.cc
old/RangeServerMetaLogReader.cc
bmz/bmz.c
lzo/minilzo.c
quicklz/quicklz.cc
)

add_library(Hypertable ${Hypertable_SRCS})
add_dependencies(Hypertable HyperComm Hyperspace HyperCommon)
target_link_libraries(Hypertable ${EXPAT_LIBRARIES} ${RRD_LIBRARIES} Hyperspace HyperDfsBroker)

# generate_test_data
add_executable(generate_test_data generate_test_data.cc)
target_link_libraries(generate_test_data Hypertable)

# old meta log dumper
add_executable(dump_old_metalog tests/DumpMetaLog.cc)
target_link_libraries(dump_old_metalog HyperDfsBroker Hypertable)

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
if (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")
   target_link_libraries(bmz-test Hypertable m)
else (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")
   target_link_libraries(bmz-test Hypertable)
endif (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")

add_executable(bmzip bmz/bmzip.c)
if (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")
   target_link_libraries(bmzip Hypertable m)
else (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")
   target_link_libraries(bmzip Hypertable)
endif (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")

# commit_log_test
add_executable(commit_log_test tests/commit_log_test.cc)
target_link_libraries(commit_log_test HyperDfsBroker Hypertable)

# escape_test
add_executable(escape_test tests/escape_test.cc)
target_link_libraries(escape_test Hypertable)

# large_insert_test
add_executable(large_insert_test tests/large_insert_test.cc)
target_link_libraries(large_insert_test Hypertable)

# row_delete_test
add_executable(row_delete_test tests/row_delete_test.cc)
target_link_libraries(row_delete_test Hypertable)

# MutatorNoLogSyncTest
add_executable(MutatorNoLogSyncTest tests/MutatorNoLogSyncTest.cc)
target_link_libraries(MutatorNoLogSyncTest Hypertable)

# periodic_flush_test
add_executable(periodic_flush_test tests/periodic_flush_test.cc)
target_link_libraries(periodic_flush_test Hypertable)

# name_id_mapper_test 
add_executable(name_id_mapper_test tests/name_id_mapper_test.cc)
target_link_libraries(name_id_mapper_test Hypertable Hyperspace)

# rangeserver_serialize_test 
add_executable(rangeserver_serialize_test tests/rangeserver_serialize_test.cc)
target_link_libraries(rangeserver_serialize_test Hypertable Hyperspace)

# MetaLog test
add_executable(metalog_test tests/metalog_test.cc)
target_link_libraries(metalog_test HyperDfsBroker Hypertable)


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

configure_file(${HYPERTABLE_SOURCE_DIR}/tests/data/random.dat
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
configure_file(${SRC_DIR}/rsmltest.golden ${DST_DIR}/rsmltest.golden
               COPYONLY)
configure_file(${SRC_DIR}/rsmltest2.golden ${DST_DIR}/rsmltest2.golden
               COPYONLY)
configure_file(${SRC_DIR}/rsmltest3.golden ${DST_DIR}/rsmltest3.golden
               COPYONLY)
configure_file(${SRC_DIR}/rsmltest4.golden ${DST_DIR}/rsmltest4.golden
               COPYONLY)
configure_file(${SRC_DIR}/mmltest.golden ${DST_DIR}/mmltest.golden
               COPYONLY)
configure_file(${SRC_DIR}/mmltest2.golden ${DST_DIR}/mmltest2.golden
               COPYONLY)
configure_file(${SRC_DIR}/mmltest3.golden ${DST_DIR}/mmltest3.golden
               COPYONLY)
configure_file(${SRC_DIR}/mmltest4.golden ${DST_DIR}/mmltest4.golden
               COPYONLY)
configure_file(${HYPERTABLE_SOURCE_DIR}/conf/hypertable.cfg
               ${DST_DIR}/hypertable.cfg)
configure_file (${SRC_DIR}/MutatorNoLogSyncTest.cfg ${DST_DIR}/MutatorNoLogSyncTest.cfg)
configure_file(${SRC_DIR}/name_id_mapper_test.cfg ${DST_DIR}/name_id_mapper_test.cfg)
configure_file(${SRC_DIR}/metalog_test.golden ${DST_DIR}/metalog_test.golden)
configure_file(${SRC_DIR}/metalog_test2.golden ${DST_DIR}/metalog_test2.golden)

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
add_test(MetaLog metalog_test)
add_test(Client-large-block large_insert_test)
add_test(Client-row-delete row_delete_test)
add_test(Client-periodic-flush periodic_flush_test)
add_test(NameIdMapper name_id_mapper_test --config=${DST_DIR}/name_id_mapper_test.cfg)
add_test(StatsRangeServer-serialize rangeserver_serialize_test)

if (NOT HT_COMPONENT_INSTALL)
  file(GLOB HEADERS *.h)

  install(FILES ${HEADERS} DESTINATION include/Hypertable/Lib)

  install(TARGETS generate_test_data RUNTIME DESTINATION bin)

  install(TARGETS Hypertable dump_old_metalog
          RUNTIME DESTINATION bin
          LIBRARY DESTINATION lib
          ARCHIVE DESTINATION lib)
endif ()
