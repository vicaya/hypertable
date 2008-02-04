#
# Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# 

# dumplog - program to display a commit log
add_executable (dumplog dumplog.cc)
target_link_libraries (dumplog pthread ${BOOST_LIB} log4cpp ${ZLIB_LIBRARIES} ${Lzo2_LIBRARIES} ${Tcmalloc_LIBRARIES} HyperDfsBroker Hypertable)

install (TARGETS dumplog RUNTIME DESTINATION bin)
