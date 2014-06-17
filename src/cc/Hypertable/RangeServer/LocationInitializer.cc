/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */
#include "Common/Compat.h"
#include "Common/Config.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/Serialization.h"
#include "Common/StatsSystem.h"
#include "Common/Path.h"

#include <boost/algorithm/string.hpp>

#include "Hypertable/Lib/MasterProtocol.h"

#include "LocationInitializer.h"

using namespace Hypertable;
using namespace Serialization;

LocationInitializer::LocationInitializer(PropertiesPtr &props) 
  : m_props(props), m_location_persisted(false) {

  Path data_dir = m_props->get_str("Hypertable.DataDirectory");
  data_dir /= "/run";
  if (!FileUtils::exists(data_dir.string()))
    FileUtils::mkdirs(data_dir.string());
  m_location_file = (data_dir /= "/location").string();

  // Get location string
  {
    m_location = m_props->get_str("Hypertable.RangeServer.ProxyName");
    if (!m_location.empty())
      boost::trim(m_location);
    else if (FileUtils::exists(m_location_file)) {
      if (FileUtils::read(m_location_file, m_location) <= 0) {
        HT_ERRORF("Problem reading location file '%s'", m_location_file.c_str());
        _exit(1);
      }
      m_location_persisted = true;
      boost::trim(m_location);
    }
  }
  
}

CommBuf *LocationInitializer::create_initialization_request() {
  ScopedLock lock(m_mutex);
  StatsSystem stats;
  const char *base, *ptr;
  String datadirs = m_props->get_str("Hypertable.RangeServer.Monitoring.DataDirectories");
  uint16_t port = m_props->get_i16("Hypertable.RangeServer.Port");
  String dir;
  std::vector<String> dirs;

  base = datadirs.c_str();
  while ((ptr = strchr(base, ',')) != 0) {
    dir = String(base, ptr-base);
    boost::trim(dir);
    dirs.push_back(dir);
    base = ptr+1;
  }
  dir = String(base);
  boost::trim(dir);
  dirs.push_back(dir);

  stats.add_categories(StatsSystem::CPUINFO|StatsSystem::NETINFO|
                       StatsSystem::OSINFO|StatsSystem::PROCINFO, dirs);

  CommHeader header(MasterProtocol::COMMAND_REGISTER_SERVER);
  CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(m_location) + 2 + stats.encoded_length());
  cbuf->append_vstr(m_location);
  cbuf->append_i16(port);
  stats.encode(cbuf->get_data_ptr_address());
  return cbuf;
}

bool LocationInitializer::process_initialization_response(Event *event) {

  if (Protocol::response_code(event) != Error::OK) {
    HT_ERROR_OUT << "Problem initializing Master connection - "
                 << Protocol::string_format_message(event) << HT_END;
    return false;
  }

  const uint8_t *ptr = event->payload + 4;
  size_t remain = event->payload_len - 4;
  String location = decode_vstr(&ptr, &remain);
  bool location_persisted = false;

  {
    ScopedLock lock(m_mutex);
    if (m_location == "")
      m_location = location;
    else
      HT_ASSERT(m_location == location);
    location_persisted = m_location_persisted;
  }

  if (!location_persisted) {
    if (FileUtils::write(m_location_file, location) < 0) {
      HT_ERRORF("Unable to write location to file '%s'", m_location_file.c_str());
      _exit(1);
    }
    {
      ScopedLock lock(m_mutex);
      m_location_persisted = true;
    }
  }
  m_cond.notify_all();
  return true;
}

String LocationInitializer::get() {
  ScopedLock lock(m_mutex);
  return m_location;
}

void LocationInitializer::wait_until_assigned() {
  ScopedLock lock(m_mutex);
  while (m_location == "")
    m_cond.wait(lock);
}
