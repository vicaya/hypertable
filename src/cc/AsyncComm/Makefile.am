#
# $Id$
#
# This file is part of Placer.
#
# Placer is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser Public License as published by
# the Free Software Foundation; either version 2.1 of the License, or
# any later version.
#
# Placer is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser Public License for more details.
#
# You should have received a copy of the GNU Lesser Public License
# along with Placer; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#

AM_CXXFLAGS = -I@top_srcdir@/src/cc

lib_LTLIBRARIES = libAsyncComm.la
libAsyncComm_la_SOURCES = CallbackHandlerSynchronizer.cc Comm.cc ConnectionManager.cc \
	Event.cc IOHandler.cc IOHandlerAccept.cc IOHandlerData.cc Message.cc MessageBuilder.cc \
	Protocol.cc Reactor.cc ReactorFactory.cc ReactorRunner.cc RequestCache.cc
libAsyncComm_la_LIBADD = -lboost_thread

bin_PROGRAMS = sampleClient testServer

sampleClient_SOURCES = sampleClient.cc
sampleClient_LDADD = -lpthread -lboost_thread -L../Common -lCommon libAsyncComm.la -llog4cpp -lboost_thread

testServer_SOURCES = testServer.cc
testServer_LDADD = -lpthread -lboost_thread libAsyncComm.la -L../Common -lCommon -llog4cpp -lboost_thread

check_PROGRAMS = commTest testServer

commTest_SOURCES = tests/commTest.cc tests/CommTestThreadFunction.cc
commTest_LDADD = -lpthread -lboost_thread libAsyncComm.la -L../Common -lCommon -llog4cpp -lboost_thread -lexpat

TESTS = commTest

clean-local:
	-rm -f tests/commTest.output.*
