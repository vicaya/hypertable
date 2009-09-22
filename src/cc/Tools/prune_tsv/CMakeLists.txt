#
# Copyright (C) 2009 Doug Judd (Zvents, Inc.)
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2 of
# the License.
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

# prune_tsv - drops lines with date string older than window
add_executable(prune_tsv prune_tsv.cc)
target_link_libraries(prune_tsv HyperCommon HyperComm)

if (NOT HT_COMPONENT_INSTALL)
  install(TARGETS prune_tsv RUNTIME DESTINATION bin)
endif ()
