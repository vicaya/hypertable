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

set ( DfsBroker_SRCS
Client.cc
ClientBufferedReaderHandler.cc
ConnectionHandler.cc
Protocol.cc
RequestHandlerClose.cc
RequestHandlerCreate.cc
RequestHandlerOpen.cc
RequestHandlerRead.cc
RequestHandlerAppend.cc
RequestHandlerSeek.cc
RequestHandlerRemove.cc
RequestHandlerLength.cc
RequestHandlerPread.cc
RequestHandlerMkdirs.cc
RequestHandlerFlush.cc
RequestHandlerStatus.cc
RequestHandlerRmdir.cc
RequestHandlerReaddir.cc
ResponseCallbackOpen.cc
ResponseCallbackRead.cc
ResponseCallbackAppend.cc
ResponseCallbackLength.cc
ResponseCallbackReaddir.cc
)

add_library (DfsBroker ${DfsBroker_SRCS})
