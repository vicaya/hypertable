/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_MAINTENANCETASKMEMORYPURGE_H
#define HYPERTABLE_MAINTENANCETASKMEMORYPURGE_H

#include "MaintenanceTask.h"

namespace Hypertable {

  class MaintenanceTaskMemoryPurge : public MaintenanceTask {
  public:
    MaintenanceTaskMemoryPurge(boost::xtime &stime, RangePtr &range);
    virtual void execute();
  };

}

#endif // HYPERTABLE_MAINTENANCETASKMEMORYPURGE_H
