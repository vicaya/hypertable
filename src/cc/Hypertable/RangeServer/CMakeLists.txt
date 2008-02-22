#
# Copyright (C) 2008 Doug Judd (Zvents, Inc.)
# 
# This file is part of Hypertable.
# 
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License.
# 
# Hypertable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# 

set ( RangeServer_SRCS
AccessGroup.cc
BlockCompressionHeaderCellStore.cc
CellCache.cc
CellCacheScanner.cc
CellStoreScannerV0.cc
CellStoreTrailerV0.cc
CellStoreV0.cc
ConnectionHandler.cc
EventHandlerMasterConnection.cc
FileBlockCache.cc
FillScanBlock.cc
Global.cc
HyperspaceSessionHandler.cc
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
RequestHandlerDumpStats.cc
RequestHandlerFetchScanblock.cc
RequestHandlerDropTable.cc
RequestHandlerLoadRange.cc
RequestHandlerStatus.cc
RequestHandlerUpdate.cc
ResponseCallbackCreateScanner.cc
ResponseCallbackFetchScanblock.cc
ResponseCallbackUpdate.cc
ScanContext.cc
ScannerMap.cc
ScannerTimestampController.cc
TableInfo.cc
TimerHandler.cc
main.cc
)

# RangeServer
add_executable (Hypertable.RangeServer ${RangeServer_SRCS})
target_link_libraries (Hypertable.RangeServer pthread ${BOOST_LIB} log4cpp expat ${ZLIB_LIBRARIES} ${Lzo2_LIBRARIES} ${Tcmalloc_LIBRARIES} HyperDfsBroker Hypertable)

# csdump
add_executable (csdump CellStoreV0.cc CellStoreTrailerV0.cc BlockCompressionHeaderCellStore.cc csdump.cc CellStoreScannerV0.cc FileBlockCache.cc Global.cc ScanContext.cc)
target_link_libraries (csdump pthread ${BOOST_LIB} log4cpp ${ZLIB_LIBRARIES} ${Lzo2_LIBRARIES} ${Tcmalloc_LIBRARIES} HyperDfsBroker Hypertable)

# count_stored - program to diff two sorted files
add_executable (count_stored count_stored.cc CellStoreV0.cc CellStoreTrailerV0.cc BlockCompressionHeaderCellStore.cc CellStoreScannerV0.cc FileBlockCache.cc Global.cc ScanContext.cc)
target_link_libraries (count_stored pthread ${BOOST_LIB} log4cpp expat ${ZLIB_LIBRARIES} ${Lzo2_LIBRARIES} ${Tcmalloc_LIBRARIES} HyperDfsBroker Hypertable)

install (TARGETS Hypertable.RangeServer csdump count_stored
	 RUNTIME DESTINATION bin)
