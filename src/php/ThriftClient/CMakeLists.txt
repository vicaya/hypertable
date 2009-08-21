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

HT_GLOB(GenPHP_SRCS gen-php/*.php)

add_custom_command(
  OUTPUT    ${GenPHP_SRCS}
  COMMAND   thrift
  ARGS      -r --gen php -o ${CMAKE_CURRENT_SOURCE_DIR}
            ${ThriftBroker_IDL_DIR}/Hql.thrift
  DEPENDS   ${ThriftBroker_IDL_DIR}/Client.thrift
            ${ThriftBroker_IDL_DIR}/Hql.thrift
  COMMENT   "thrift2php"
)

add_custom_target(ThriftGenPHP ALL DEPENDS ${GenPHP_SRCS})

if (PHPTHRIFT_FOUND)
  add_test(ThriftClient-php env PHPTHRIFT_ROOT=${PHPTHRIFT_ROOT}
           php ${CMAKE_CURRENT_SOURCE_DIR}/client_test.php)
endif ()

if (NOT HT_COMONENT_INSTALL OR PACKAGE_THRIFTBROKER)
  install(DIRECTORY gen-php DESTINATION lib/php)
  install(FILES client_test.php ThriftClient.php DESTINATION lib/php)
endif ()
