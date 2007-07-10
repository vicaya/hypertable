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

AM_CXXFLAGS = -I@top_srcdir@/src/cc

lib_LTLIBRARIES = libHyperspace.la
libHyperspace_la_SOURCES = HyperspaceClient.cc HyperspaceProtocol.cc

bin_PROGRAMS = hyperspace

hyperspace_SOURCES = tests/hyperspace.cc tests/CommandCreate.cc tests/CommandMkdirs.cc \
	tests/CommandAttrSet.cc tests/CommandAttrGet.cc tests/CommandAttrDel.cc \
	tests/CommandExists.cc
hyperspace_LDADD = libHyperspace.la -L../Common -lCommon -L../AsyncComm -lAsyncComm -llog4cpp
hyperspace_LDFLAGS = -dynamix
