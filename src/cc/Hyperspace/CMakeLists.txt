#
# Copyright(C) 2008 Doug Judd (Zvents, Inc.)
#
# This file is part of Hypertable.
#
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2 of
# the License.
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

set(Hyperspace_SRCS
ClientKeepaliveHandler.cc
ClientConnectionHandler.cc
Config.cc
DirEntry.cc
HandleCallback.cc
Protocol.cc
Session.cc
HsCommandInterpreter.cc
HsHelpText.cc
HsClientState.cc
)

# Hyperspace library
add_library(Hyperspace ${Hyperspace_SRCS})
target_link_libraries(Hyperspace ${BDB_LIBRARIES} HyperTools)

set(Master_SRCS
StateDbKeys.cc
BerkeleyDbFilesystem.cc
Event.cc
Master.cc
RequestHandlerMkdir.cc
RequestHandlerDelete.cc
RequestHandlerExpireSessions.cc
RequestHandlerRenewSession.cc
RequestHandlerOpen.cc
RequestHandlerClose.cc
RequestHandlerAttrSet.cc
RequestHandlerAttrGet.cc
RequestHandlerAttrExists.cc
RequestHandlerAttrList.cc
RequestHandlerAttrDel.cc
RequestHandlerExists.cc
RequestHandlerReaddir.cc
RequestHandlerLock.cc
RequestHandlerRelease.cc
RequestHandlerStatus.cc
RequestHandlerHandshake.cc
ResponseCallbackOpen.cc
ResponseCallbackExists.cc
ResponseCallbackAttrGet.cc
ResponseCallbackAttrExists.cc
ResponseCallbackAttrList.cc
ResponseCallbackLock.cc
ResponseCallbackReaddir.cc
ServerConnectionHandler.cc
ServerKeepaliveHandler.cc
main.cc
)

# Hyperspace.Master
add_executable(Hyperspace.Master ${Master_SRCS})
target_link_libraries(Hyperspace.Master Hyperspace ${MALLOC_LIBRARY})

# BerkeleyDbFilesystem test
add_executable(bdb_fs_test tests/bdb_fs_test.cc BerkeleyDbFilesystem.cc StateDbKeys.cc)
target_link_libraries(bdb_fs_test ${BDB_LIBRARIES} HyperCommon)

#
# Copy test files
#
set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Hyperspace/tests")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Hyperspace")
configure_file(${SRC_DIR}/bdb_fs_test.golden ${DST_DIR}/bdb_fs_test.golden)

add_test(BerkeleyDbFilesystem bdb_fs_test)

if (NOT HT_COMPONENT_INSTALL)
  file(GLOB HEADERS *.h)

  install(FILES ${HEADERS} DESTINATION include/Hyperspace)

  install(TARGETS Hyperspace Hyperspace.Master
          RUNTIME DESTINATION bin
          LIBRARY DESTINATION lib
          ARCHIVE DESTINATION lib)
endif ()
