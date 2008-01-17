/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include "MaintenanceTaskCompaction.h"

using namespace Hypertable;

/**
 *
 */
MaintenanceTaskCompaction::MaintenanceTaskCompaction(RangePtr &range_ptr, bool major) : MaintenanceTask(), m_range_ptr(range_ptr), m_major(major) {
}


/**
 *
 */
void MaintenanceTaskCompaction::execute() {
  m_range_ptr->do_compaction(m_major);
}
