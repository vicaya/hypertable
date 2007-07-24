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

set ( SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Hypertable/RangeServer" )
set ( DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Hypertable/RangeServer" )

set ( TEST_DEPENDENCIES ${DST_DIR}/shakespeare.txt ${DST_DIR}/words ${DST_DIR}/urls.txt ${DST_DIR}/tests )

set ( RangeServer_SRCS
../Schema.cc
AccessGroup.cc
BlockDeflaterZlib.cc
BlockInflaterZlib.cc
CellCache.cc
CellCacheScanner.cc
CellListScanner.cc
CellStore.cc
CellStoreScannerV0.cc
CellStoreV0.cc
CommitLog.cc
CommitLogLocal.cc
CommitLogReaderLocal.cc
ConnectionHandler.cc
Constants.cc
FileBlockCache.cc
FillScanBlock.cc
Global.cc
Key.cc
MaintenanceThread.cc
MergeScanner.cc
Range.cc
RangeServer.cc
RangeServerProtocol.cc
Request.cc
RequestHandlerCompact.cc
RequestHandlerCreateScanner.cc
RequestHandlerFetchScanblock.cc
RequestHandlerLoadRange.cc
RequestHandlerUpdate.cc
ScannerMap.cc
SplitLogMap.cc
TableInfo.cc
VerifySchema.cc
main.cc
ResponseCallbackCreateScanner.cc
)

# RangeServer
add_executable (Hypertable.RangeServer ${RangeServer_SRCS})
target_link_libraries (Hypertable.RangeServer pthread boost_thread Hyperspace HdfsClient Test AsyncComm Common log4cpp expat ${ZLIB_LIBRARIES})

# cellCacheTest
add_executable (cellCacheTest CellCache.cc CellCacheScanner.cc tests/cellCacheTest.cc CellListScanner.cc Key.cc ${TEST_DEPENDENCIES})
target_link_libraries (cellCacheTest pthread boost_thread AsyncComm Common log4cpp)

# cellStoreDump
add_executable (cellStoreDump Constants.cc CellStoreV0.cc BlockDeflaterZlib.cc BlockInflaterZlib.cc cellStoreDump.cc CellListScanner.cc CellStoreScannerV0.cc Key.cc FileBlockCache.cc Global.cc)
target_link_libraries (cellStoreDump pthread boost_thread HdfsClient AsyncComm Common log4cpp ${ZLIB_LIBRARIES})

# cellStoreTest
add_executable (cellStoreTest Constants.cc CellStoreV0.cc BlockDeflaterZlib.cc BlockInflaterZlib.cc tests/cellStoreTest.cc CellListScanner.cc CellStoreScannerV0.cc CellCache.cc CellCacheScanner.cc Key.cc tests/TestSource.cc FileBlockCache.cc Global.cc CellStore.cc ${TEST_DEPENDENCIES})
target_link_libraries (cellStoreTest pthread boost_thread HdfsClient AsyncComm Common log4cpp ${ZLIB_LIBRARIES})

# cellStoreTest2
add_executable (cellStoreTest2 Constants.cc CellStoreV0.cc BlockDeflaterZlib.cc BlockInflaterZlib.cc tests/cellStoreTest2.cc CellListScanner.cc CellStoreScannerV0.cc CellCache.cc CellCacheScanner.cc Key.cc tests/TestSource.cc ../Schema.cc MergeScanner.cc FileBlockCache.cc Global.cc CellStore.cc ${TEST_DEPENDENCIES})
target_link_libraries (cellStoreTest2 pthread boost_thread HdfsClient AsyncComm Common log4cpp ${ZLIB_LIBRARIES} expat)

# generateTestData
add_executable (generateTestData tests/generateTestData.cc ../Schema.cc)
target_link_libraries (generateTestData pthread boost_thread Common log4cpp expat)

# commitLogReaderTest
add_executable (commitLogReaderTest CommitLogReaderLocal.cc tests/commitLogReaderTest.cc)
target_link_libraries (commitLogReaderTest pthread boost_thread Common log4cpp)

#
# Get test environment setup
#
add_custom_command (SOURCE ${SRC_DIR}/tests/urls.txt.gz
                    COMMAND cp ARGS ${SRC_DIR}/tests/urls.txt.gz ${DST_DIR}
                    COMMAND gzip ARGS -d ${DST_DIR}/urls.txt.gz
                    OUTPUT ${DST_DIR}/urls.txt)
add_custom_command (SOURCE ${SRC_DIR}/tests/words.gz
                    COMMAND cp ARGS ${SRC_DIR}/tests/words.gz ${DST_DIR}
                    COMMAND gzip ARGS -d ${DST_DIR}/words.gz
                    OUTPUT ${DST_DIR}/words)
add_custom_command (SOURCE ${SRC_DIR}/tests/shakespeare.txt.gz
                    COMMAND cp ARGS ${SRC_DIR}/tests/shakespeare.txt.gz ${DST_DIR}
                    COMMAND gzip ARGS -d ${DST_DIR}/shakespeare.txt.gz
                    OUTPUT ${DST_DIR}/shakespeare.txt)
add_custom_command (SOURCE ${SRC_DIR}/tests
                    COMMAND ln ARGS -s ${SRC_DIR}/tests ${DST_DIR}/tests
                    OUTPUT ${DST_DIR}/tests)

add_test (CellStoreTest cellStoreTest)
add_test (CellStoreTest2 cellStoreTest2)
add_test (CellCacheTest cellCacheTest)

install (TARGETS Hypertable.RangeServer cellStoreDump generateTestData
	 RUNTIME DESTINATION bin)
