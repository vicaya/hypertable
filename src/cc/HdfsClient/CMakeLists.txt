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

add_library (HdfsClient HdfsClient.cc HdfsProtocol.cc)

# testClient
add_executable (testClient testClient.cc CommandCopyFromLocal.cc CommandCopyToLocal.cc CommandRemove.cc CommandShutdown.cc CommandLength.cc CommandMkdirs.cc Global.cc)
target_link_libraries (testClient pthread ${BOOST_LIB} HdfsClient AsyncComm Common log4cpp)

# hdfsTest
add_executable (hdfsTest CommandCopyFromLocal.cc CommandCopyToLocal.cc CommandRemove.cc CommandShutdown.cc CommandLength.cc CommandMkdirs.cc Global.cc tests/hdfsTest.cc tests/HdfsTestThreadFunction.cc)
target_link_libraries (hdfsTest pthread ${BOOST_LIB} HdfsClient AsyncComm Common log4cpp)

add_test (HdfsClient hdfsTest)
