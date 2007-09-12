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

set ( SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Tools/rsclient" )
set ( DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Tools/rsclient" )

set ( TEST_DEPENDENCIES ${DST_DIR}/testdata.txt )

set (rsclient_SRCS
rsclient.cc
CommandCreateScanner.cc
CommandFetchScanblock.cc
CommandLoadRange.cc
CommandUpdate.cc
DisplayScanData.cc
FetchSchema.cc
Global.cc
ParseRangeSpec.cc
)

# rsclient - Range Server client command interpreter
add_executable (rsclient ${rsclient_SRCS})
target_link_libraries (rsclient pthread ${BOOST_LIB} Hypertable AsyncComm Hyperspace DfsBroker Common log4cpp expat readline curses)

# rsTest
add_executable (rsTest tests/rsTest.cc ${TEST_DEPENDENCIES} )
target_link_libraries (rsTest pthread ${BOOST_LIB} Common log4cpp)

install (TARGETS rsclient RUNTIME DESTINATION bin)

add_custom_command (SOURCE ${SRC_DIR}/tests/testdata.txt.gz
                    COMMAND cp ARGS ${SRC_DIR}/tests/testdata.txt.gz ${DST_DIR}
                    COMMAND gunzip ARGS ${DST_DIR}/testdata.txt.gz
                    OUTPUT ${DST_DIR}/testdata.txt)

configure_file(${HYPERTABLE_SOURCE_DIR}/conf/hypertable.cfg  ${DST_DIR}/hypertable.cfg)
configure_file(${SRC_DIR}/tests/rsTest.golden ${DST_DIR}/rsTest.golden)

add_test (RangeServer rsTest)
