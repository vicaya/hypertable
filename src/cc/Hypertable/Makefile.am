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

SUBDIRS = HdfsClient Test Master RangeServer 

check_PROGRAMS = schemaTest

schemaTest_SOURCES = tests/schemaTest.cc Schema.cc
schemaTest_LDADD = -lpthread -lboost_thread -L../Common -lCommon -llog4cpp -lboost_thread -lexpat

TESTS = schemaTest
