#
# Copyright (C) 2008 Doug Judd (Zvents, Inc.)
#
# This file is part of Hypertable.
#
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License, or any later version.
#
# Hypertable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#

set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Hypertable/RangeServer/tests")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Hypertable/RangeServer")

set(TEST_DEPENDENCIES ${DST_DIR}/words)

set(RangeServer_SRCS
AccessGroup.cc
CellCache.cc
CellCachePool.cc
CellStoreReleaseCallback.cc
CellCacheScanner.cc
CellStoreScannerV0.cc
CellStoreTrailerV0.cc
CellStoreV0.cc
Config.cc
ConnectionHandler.cc
EventHandlerMasterConnection.cc
FileBlockCache.cc
FillScanBlock.cc
Global.cc
HyperspaceSessionHandler.cc
MaintenanceQueue.cc
MaintenanceTaskCompaction.cc
MaintenanceTaskLogCleanup.cc
MaintenanceTaskSplit.cc
MergeScanner.cc
MetadataNormal.cc
MetadataRoot.cc
Range.cc
RangeServer.cc
RequestHandlerCompact.cc
RequestHandlerCreateScanner.cc
RequestHandlerDestroyScanner.cc
RequestHandlerDropRange.cc
RequestHandlerDumpStats.cc
RequestHandlerGetStatistics.cc
RequestHandlerFetchScanblock.cc
RequestHandlerDropTable.cc
RequestHandlerLoadRange.cc
RequestHandlerReplayBegin.cc
RequestHandlerReplayLoadRange.cc
RequestHandlerReplayUpdate.cc
RequestHandlerReplayCommit.cc
RequestHandlerStatus.cc
RequestHandlerUpdate.cc
RequestHandlerShutdown.cc
ResponseCallbackCreateScanner.cc
ResponseCallbackFetchScanblock.cc
ResponseCallbackGetStatistics.cc
ResponseCallbackUpdate.cc
ScanContext.cc
ScannerMap.cc
TableInfo.cc
TableInfoMap.cc
TimerHandler.cc
)

# RangeServer Lib
add_library(HyperRanger ${RangeServer_SRCS})
target_link_libraries(HyperRanger m HyperDfsBroker)

# RangeServer
add_executable(Hypertable.RangeServer main.cc)
target_link_libraries(Hypertable.RangeServer HyperRanger ${MALLOC_LIBRARY})

# csdump
add_executable(csdump csdump.cc)
target_link_libraries(csdump HyperRanger)

# count_stored - program to diff two sorted files
add_executable(count_stored count_stored.cc)
target_link_libraries(count_stored HyperRanger)

# FileBlockCache test
add_executable(FileBlockCache_test tests/FileBlockCache_test.cc)
target_link_libraries(FileBlockCache_test HyperRanger)

# CellStoreScanner test
add_executable(CellStoreScanner_test tests/CellStoreScanner_test.cc
               ${TEST_DEPENDENCIES})
target_link_libraries(CellStoreScanner_test HyperRanger)

configure_file(${SRC_DIR}/CellStoreScanner_test.golden
               ${DST_DIR}/CellStoreScanner_test.golden)

add_custom_command(SOURCE ${HYPERTABLE_SOURCE_DIR}/tests/data/words.gz
    COMMAND gzip ARGS -d < ${HYPERTABLE_SOURCE_DIR}/tests/data/words.gz
                         > ${DST_DIR}/words
    OUTPUT ${DST_DIR}/words)

set(ADDITIONAL_MAKE_CLEAN_FILES ${DST_DIR}/words)

add_test(FileBlockCache FileBlockCache_test)
add_test(CellStoreScanner CellStoreScanner_test)

install(TARGETS HyperRanger Hypertable.RangeServer csdump count_stored
        RUNTIME DESTINATION ${VERSION}/bin
        LIBRARY DESTINATION ${VERSION}/lib
        ARCHIVE DESTINATION ${VERSION}/lib)
