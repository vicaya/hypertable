/** -*- C++ -*-
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

#ifndef HYPERTABLE_LIB_CONFIG_H
#define HYPERTABLE_LIB_CONFIG_H

#include "Common/Config.h"

namespace Hypertable { namespace Config {

  void init_comm_options();
  void init_comm();

  inline void init_with_comm(int argc, char *argv[], const Desc *desc = NULL,
      const Desc *hidden = NULL, const PositionalDesc *p = NULL) {
    System::initialize(System::locate_install_dir(argv[0]));
    if (desc) description(*desc);
    init_default_options();
    init_comm_options();
    parse_args(argc, argv, desc, hidden, p);
    init_default_actions();
    init_comm();
  }

}} // namespace Hypertable::Config

#endif // HYPERTABLE_LIB_CONFIG_H
