#
# Copyright(C) 2008 Doug Judd (Zvents, Inc.)
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

set(GenPerl_DIR ${CMAKE_CURRENT_SOURCE_DIR}/gen-perl/Hypertable)

set(GenPerl_SRCS
  ${GenPerl_DIR}/ThriftGen/ClientService.pm
  ${GenPerl_DIR}/ThriftGen2/HqlService.pm
  ${GenPerl_DIR}/Hypertable/ThriftGen/Constants.pm
  ${GenPerl_DIR}/Hypertable/ThriftGen/Types.pm
  ${GenPerl_DIR}/Hypertable/ThriftGen2/Constants.pm
  ${GenPerl_DIR}/Hypertable/ThriftGen2/Types.pm
)

add_custom_command(
  OUTPUT    ${GenPerl_SRCS}
  COMMAND   thrift
  ARGS      -r --gen perl -o ${CMAKE_CURRENT_SOURCE_DIR}
            ${ThriftBroker_IDL_DIR}/Hql.thrift
  DEPENDS   ${ThriftBroker_IDL_DIR}/Client.thrift
            ${ThriftBroker_IDL_DIR}/Hql.thrift
  COMMENT   "thrift2perl"
)

add_custom_target(ThriftGenPerl ALL DEPENDS ${GenPerl_SRCS})

add_test(ThriftClient-perl perl -I ${CMAKE_CURRENT_SOURCE_DIR}
         -I ${CMAKE_CURRENT_SOURCE_DIR}/gen-perl
         ${CMAKE_CURRENT_SOURCE_DIR}/client_test.pl)
