#
# Copyright 2007 Doug Judd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at 
#
# http://www.apache.org/licenses/LICENSE-2.0 
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

AUTOMAKE_OPTIONS = foreign

AM_CXXFLAGS = -I@top_srcdir@/src/cc

bin_PROGRAMS = Hypertable.RangeServer cellStoreTest generateTestData commitLogReaderTest cellStoreDump

Hypertable_RangeServer_SOURCES = AccessGroup.cc BlockDeflaterZlib.cc BlockInflaterZlib.cc CellCache.cc \
	CellCacheScanner.cc CellListScanner.cc CellStore.cc CellStoreScannerV0.cc CellStoreV0.cc \
	CommitLog.cc CommitLogLocal.cc CommitLogReaderLocal.cc ConnectionHandler.cc Constants.cc \
	FileBlockCache.cc FillScanBlock.cc Global.cc Key.cc MaintenanceThread.cc MergeScanner.cc Range.cc \
	RangeServer.cc RangeServerProtocol.cc RequestCompact.cc RequestCreateScanner.cc RequestFactory.cc \
	RequestFetchScanblock.cc RequestLoadRange.cc RequestUpdate.cc ScannerMap.cc SplitLogMap.cc \
	TableInfo.cc VerifySchema.cc ../Schema.cc
Hypertable_RangeServer_LDADD = -lpthread -lboost_thread -L../../AsyncComm -lAsyncComm -L../../Common -lCommon \
	-L../../Hyperspace -lHyperspace -lHdfsClient -L../HdfsClient -lTest -L../Test -llog4cpp -lexpat -lz

check_PROGRAMS = cellStoreTest cellCacheTest cellStoreTest2

cellCacheTest_SOURCES = CellCache.cc CellCacheScanner.cc tests/cellCacheTest.cc CellListScanner.cc Key.cc
cellCacheTest_LDADD = -lpthread -lboost_thread -L../../AsyncComm -lAsyncComm -L../../Common -lCommon -llog4cpp -lboost_thread -lexpat

cellStoreDump_SOURCES = Constants.cc CellStoreV0.cc BlockDeflaterZlib.cc BlockInflaterZlib.cc cellStoreDump.cc \
	CellListScanner.cc CellStoreScannerV0.cc Key.cc FileBlockCache.cc Global.cc
cellStoreDump_LDADD = -lpthread -lboost_thread -L../../Common -lCommon -L../../AsyncComm -lAsyncComm \
	-L../HdfsClient -lHdfsClient -llog4cpp -lboost_thread -lz

cellStoreTest_SOURCES = Constants.cc CellStoreV0.cc BlockDeflaterZlib.cc BlockInflaterZlib.cc tests/cellStoreTest.cc \
	CellListScanner.cc CellStoreScannerV0.cc CellCache.cc CellCacheScanner.cc Key.cc tests/TestSource.cc FileBlockCache.cc \
	Global.cc CellStore.cc
cellStoreTest_LDADD = -lpthread -lboost_thread -L../../Common -lCommon -L../../AsyncComm -lAsyncComm -L../HdfsClient -lHdfsClient -llog4cpp -lboost_thread -lz

cellStoreTest2_SOURCES = Constants.cc CellStoreV0.cc BlockDeflaterZlib.cc BlockInflaterZlib.cc tests/cellStoreTest2.cc \
	CellListScanner.cc CellStoreScannerV0.cc CellCache.cc CellCacheScanner.cc Key.cc tests/TestSource.cc ../Schema.cc \
	MergeScanner.cc FileBlockCache.cc Global.cc CellStore.cc
cellStoreTest2_LDADD = -lpthread -lboost_thread -L../../Common -lCommon -L../../AsyncComm -lAsyncComm -L../HdfsClient \
	-lHdfsClient -llog4cpp -lboost_thread -lexpat -lz

generateTestData_SOURCES = tests/generateTestData.cc ../Schema.cc 
generateTestData_LDADD = -L../../Common -lCommon -llog4cpp -lboost_thread -lexpat

commitLogReaderTest_SOURCES = CommitLogReaderLocal.cc tests/commitLogReaderTest.cc
commitLogReaderTest_LDADD = -L../../Common -lCommon -llog4cpp -lboost_thread

TESTS = cellStoreTest cellCacheTest cellStoreTest2

clean-local:
	-rm -f tests/output.*

