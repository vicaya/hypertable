/**
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
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
#include "AsyncComm/ReactorFactory.h"
#include "Hypertable/Lib/Config.h"

namespace Hypertable { namespace Config {

void init_comm_options() {
  description().add_options()
    ("workers", value<int>(), "Number of worker threads")
    ("reactors", value<int>(), "Number of reactor threads")
    ("timeout,t", value<int>()->default_value(10), "Timeout in seconds")
    ;
}

void init_comm() {
  int reactors = varmap.count("reactors") ? varmap["reactors"].as<int>() :
      System::get_processor_count();

  ReactorFactory::initialize(reactors);
}

}} // namespace Hypertable::Config
