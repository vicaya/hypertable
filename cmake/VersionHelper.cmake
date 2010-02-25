# Copyright (C) 2009  Luke Lu (llu@hypertable.org)
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
# along with Hypertable. If not, see <http://www.gnu.org/licenses/>
#
# figure out version info from the repository

if (VERSION_ADD_COMMIT_SUFFIX)
  exec_program(${HYPERTABLE_SOURCE_DIR}/bin/src-utils/ver ${HYPERTABLE_SOURCE_DIR}
               ARGS "--commit-only" OUTPUT_VARIABLE HT_COMMIT_STRING)
  set(VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_MICRO}.${VERSION_PATCH}.${HT_COMMIT_STRING})
else ()
  set(VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_MICRO}.${VERSION_PATCH})
endif ()


exec_program(${HYPERTABLE_SOURCE_DIR}/bin/src-utils/ver ${HYPERTABLE_SOURCE_DIR}
             OUTPUT_VARIABLE HT_GIT_VERSION RETURN_VALUE GIT_RETURN)

if (GIT_RETURN STREQUAL "0")
  set(HT_VCS_STRING ${HT_GIT_VERSION})
else ()
  set(HT_VCS_STRING "exported")
endif ()

set(VERSION_STRING "Hypertable ${VERSION} (${HT_VCS_STRING})")
