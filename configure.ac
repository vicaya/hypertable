#                                               -*- Autoconf -*-
#
# Copyright 2007 Doug Judd (Zvents, Inc.)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at 
#
# http://www.apache.org/licenses/LICENSE-2.0 
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

AC_PREREQ(2.59)
AC_INIT([Hypertable], [0.1], [doug@zvents.com])
AC_CONFIG_AUX_DIR([build-aux])
AM_INIT_AUTOMAKE
AC_PROG_MAKE_SET
AC_CONFIG_SRCDIR([COPYING])
AC_CONFIG_HEADER([config.h])

AM_PATH_LOG4CPP

#CXXFLAGS="-Wno-variadic-macros --pedantic -Wall -g -I/usr/local/include/boost-1_33_1"
CXXFLAGS="-Wno-variadic-macros -Wall -g -I/usr/local/include/boost-1_33_1"

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC

# Checks for libraries.
AC_PROG_LIBTOOL
AC_PROG_RANLIB

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 src/cc/Makefile
                 src/cc/AsyncComm/Makefile
                 src/cc/Hypertable/Makefile
                 src/cc/Hypertable/HdfsClient/Makefile
                 src/cc/Hypertable/Master/Makefile
                 src/cc/Hypertable/RangeServer/Makefile
                 src/cc/Hypertable/Test/Makefile
                 src/cc/Hyperspace/Makefile
                 src/cc/Common/Makefile])
AC_OUTPUT
