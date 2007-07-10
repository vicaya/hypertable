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

AUTOMAKE_OPTIONS = foreign

AM_CXXFLAGS = -I@top_srcdir@/src/cc

lib_LTLIBRARIES = libTest.la
libTest_la_SOURCES = Metadata.cc

bin_PROGRAMS = MetadataTest

MetadataTest_SOURCES = MetadataTest.cc 
MetadataTest_LDADD = -lpthread -lboost_thread -L../../Common -lCommon -llog4cpp libTest.la -lexpat 

