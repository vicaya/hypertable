#
# Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#

set(Master_SRCS
ConnectionHandler.cc
DispatchHandlerDropTable.cc
DispatchHandlerUpdateSchema.cc
DispatchHandlerGetStatistics.cc
EventHandlerServerJoined.cc
EventHandlerServerLeft.cc
Master.cc
Monitoring.cc
RequestHandlerCreateNamespace.cc
RequestHandlerDropNamespace.cc
RequestHandlerCreateTable.cc
RequestHandlerAlterTable.cc
RequestHandlerDropTable.cc
RequestHandlerRenameTable.cc
RequestHandlerDoMaintenance.cc
RequestHandlerGetSchema.cc
RequestHandlerStatus.cc
RequestHandlerRegisterServer.cc
RequestHandlerReportSplit.cc
RequestHandlerClose.cc
RequestHandlerShutdown.cc
ResponseCallbackGetSchema.cc
ResponseCallbackRegisterServer.cc
ServerLockFileHandler.cc
ServersDirectoryHandler.cc
MasterGc.cc
main.cc
)

# Hypertable.Master
add_executable(Hypertable.Master ${Master_SRCS})
target_link_libraries(Hypertable.Master Hyperspace Hypertable HyperDfsBroker ${RRD_LIBRARIES} ${MALLOC_LIBRARY})

add_executable(htgc htgc.cc MasterGc.cc)
target_link_libraries(htgc HyperDfsBroker Hypertable ${RRD_LIBRARIES})

if (NOT HT_COMPONENT_INSTALL)
  install(TARGETS Hypertable.Master htgc RUNTIME DESTINATION bin)
endif ()
