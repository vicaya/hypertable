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

set(GenRb_DIR ${CMAKE_CURRENT_SOURCE_DIR}/hypertable/gen-rb)

set(GenRb_SRCS
  ${GenRb_DIR}/ClientService.rb
  ${GenRb_DIR}/Client_constants.rb
  ${GenRb_DIR}/Client_types.rb
  ${GenRb_DIR}/HqlService.rb
  ${GenRb_DIR}/Hql_constants.rb
  ${GenRb_DIR}/Hql_types.rb
)

add_custom_command(
  OUTPUT    ${GenRb_SRCS}
  COMMAND   thrift
  ARGS      -r --gen rb -o ${CMAKE_CURRENT_SOURCE_DIR}/hypertable
            ${ThriftBroker_IDL_DIR}/Hql.thrift
  DEPENDS   ${ThriftBroker_IDL_DIR}/Client.thrift
            ${ThriftBroker_IDL_DIR}/Hql.thrift
  COMMENT   "thrift2rb"
)

add_custom_target(ThriftGenRb ALL DEPENDS ${GenRb_SRCS})

add_test(ThriftClient-ruby ${CMAKE_CURRENT_SOURCE_DIR}/client_test.rb)
