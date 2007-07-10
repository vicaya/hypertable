#
# Copyright 2007 Doug Judd (Zvents, Inc.)
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

lib_LTLIBRARIES = libHdfsClient.la
libHdfsClient_la_SOURCES = HdfsClient.cc HdfsProtocol.cc 

noinst_PROGRAMS = testClient

testClient_SOURCES = testClient.cc CommandCopyFromLocal.cc CommandCopyToLocal.cc CommandRemove.cc CommandShutdown.cc \
	CommandLength.cc CommandMkdirs.cc Global.cc
testClient_LDADD = -lpthread -lboost_thread -L../../Common -lCommon -L../../AsyncComm -lAsyncComm libHdfsClient.la -llog4cpp -lpthread -lboost_thread

check_PROGRAMS = hdfsTest

hdfsTest_SOURCES = CommandCopyFromLocal.cc CommandCopyToLocal.cc CommandRemove.cc CommandShutdown.cc \
	CommandLength.cc CommandMkdirs.cc Global.cc tests/hdfsTest.cc tests/HdfsTestThreadFunction.cc
hdfsTest_LDADD = -lpthread -lboost_thread -L../../AsyncComm -lAsyncComm -L../../Common -lCommon libHdfsClient.la -llog4cpp -lboost_thread -lexpat

TESTS = hdfsTest

clean-local:
	-rm -f tests/output.*

