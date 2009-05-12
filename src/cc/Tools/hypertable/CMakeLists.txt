#
# Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Tools/hypertable")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Tools/hypertable")

set(hypertable_SRCS
hypertable.cc
)

# hypertable - command interpreter
add_executable(hypertable ${hypertable_SRCS})
target_link_libraries(hypertable Hypertable)

# hypertable_test
add_executable(hypertable_test test/hypertable_test.cc ${TEST_DEPENDENCIES})
target_link_libraries(hypertable_test HyperComm)

install(TARGETS hypertable RUNTIME DESTINATION ${VERSION}/bin)

configure_file(${HYPERTABLE_SOURCE_DIR}/conf/hypertable.cfg
               ${DST_DIR}/hypertable.cfg)
configure_file(${SRC_DIR}/test/hypertable_test.golden
               ${DST_DIR}/hypertable_test.golden)
configure_file(${SRC_DIR}/test/hypertable_test.hql
               ${DST_DIR}/hypertable_test.hql)
configure_file(${SRC_DIR}/test/hypertable_test.tsv
               ${DST_DIR}/hypertable_test.tsv)

add_test(Hypertable-shell hypertable_test)

# hypertable_ldi_stdin_test
add_executable(hypertable_ldi_stdin_test test/hypertable_ldi_stdin_test.cc ${TEST_DEPENDENCIES})
target_link_libraries(hypertable_ldi_stdin_test HyperComm)

configure_file(${SRC_DIR}/test/hypertable_ldi_stdin_test_load.hql
               ${DST_DIR}/hypertable_ldi_stdin_test_load.hql)
configure_file(${SRC_DIR}/test/hypertable_ldi_stdin_test_select.hql
               ${DST_DIR}/hypertable_ldi_stdin_test_select.hql)
configure_file(${SRC_DIR}/test/hypertable_ldi_stdin_test.golden
               ${DST_DIR}/hypertable_ldi_stdin_test.golden)

add_test(Hypertable-shell-ldi-stdin hypertable_ldi_stdin_test)
