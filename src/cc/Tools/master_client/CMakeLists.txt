#
# Copyright (C) 2011 Hypertable, Inc.
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

set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Tools/master_client")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Tools/master_client")

set(master_client_SRCS
ht_master_client.cc
MasterCommandInterpreter.cc
)

# ht_master_client - Master client command interpreter
add_executable(ht_master_client ${master_client_SRCS})
target_link_libraries(ht_master_client Hypertable)

if (NOT HT_COMPONENT_INSTALL)
  install(TARGETS ht_master_client RUNTIME DESTINATION bin)
endif ()
