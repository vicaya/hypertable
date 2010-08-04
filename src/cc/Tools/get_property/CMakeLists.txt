#
# Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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

set(get_property_SRCS
get_property.cc
)

# get_property - program for outputting the value of a property
add_executable(get_property ${get_property_SRCS})
target_link_libraries(get_property HyperCommon HyperComm)

if (NOT HT_COMPONENT_INSTALL)
  install(TARGETS get_property RUNTIME DESTINATION bin)
endif ()
