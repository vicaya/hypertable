#
# Copyright (C) 2007 Doug Judd (Zvents, Inc.)
# 
# This file is part of Hypertable.
# 
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or any later version.
# 
# Hypertable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# 

# schemaTest
add_executable (schemaTest tests/schemaTest.cc Schema.cc)
target_link_libraries (schemaTest Common log4cpp expat)

#
# Copy test files
#
set (SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Hypertable/tests")
set (DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Hypertable")
configure_file(${SRC_DIR}/bad-schema-1.xml ${DST_DIR}/bad-schema-1.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-2.xml ${DST_DIR}/bad-schema-2.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-3.xml ${DST_DIR}/bad-schema-3.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-4.xml ${DST_DIR}/bad-schema-4.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-5.xml ${DST_DIR}/bad-schema-5.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-6.xml ${DST_DIR}/bad-schema-6.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-7.xml ${DST_DIR}/bad-schema-7.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-8.xml ${DST_DIR}/bad-schema-8.xml COPYONLY)
configure_file(${SRC_DIR}/bad-schema-9.xml ${DST_DIR}/bad-schema-9.xml COPYONLY)
configure_file(${SRC_DIR}/good-schema-1.xml ${DST_DIR}/good-schema-1.xml COPYONLY)
configure_file(${SRC_DIR}/schemaTest.golden ${DST_DIR}/schemaTest.golden)

add_test (Schema schemaTest)
