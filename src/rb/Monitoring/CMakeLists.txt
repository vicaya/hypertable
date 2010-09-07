#
# Copyright(C) 2010 Sanjit Jhala (Hypertable, Inc.)
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

install(FILES htmonitoring.rb config.ru DESTINATION Monitoring)
install(PROGRAMS htm-app DESTINATION Monitoring)
install(DIRECTORY app DESTINATION Monitoring)

install(CODE "CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/app/config/config.yml.in ${INSTALL_DIR}/Monitoring/app/config/config.yml)")
