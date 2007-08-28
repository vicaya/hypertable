/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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

#include "Mutator.h"

/**
 * 
 */
Mutator::Mutator(InstanceDataPtr &instPtr, SchemaPtr &schemaPtr) : mInstPtr(instPtr), mSchemaPtr(schemaPtr) {
  return;
}


/**
 *
 */
void Mutator::Set(CellKey &key, uint8_t *value, uint32_t valueLen) {

  // Location Cache lookup

  // If failed, Lookup range and Load cache

  // Insert mod into per-range queue

  // Update memory used

}


/**
 *
 */
void Mutator::Flush(MutationResultPtr &resultPtr) {

  // Sweep through the set of per-range queues, sending UPDATE requests to their range servers

  // Increment useCount variable in callback, once for each request that went out

  // 

}
