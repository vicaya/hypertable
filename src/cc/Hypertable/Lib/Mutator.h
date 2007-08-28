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

#ifndef HYPERTABLE_MUTATOR_H
#define HYPERTABLE_MUTATOR_H

#include "Common/ReferenceCount.h"

#include "CellKey.h"
#include "InstanceData.h"
#include "MutationResult.h"
#include "Schema.h"

namespace hypertable {

  class Mutator : public ReferenceCount {

  public:
    Mutator(InstanceDataPtr &instPtr, SchemaPtr &schemaPtr);
    virtual ~Mutator() { return; }

    void Set(CellKey &key, uint8_t *value, uint32_t valueLen);

    void Flush(MutationResultPtr &resultPtr);

    uint64_t MemoryUsed();

  private:
    InstanceDataPtr mInstPtr;
    SchemaPtr mSchemaPtr;
  };
  typedef boost::intrusive_ptr<Mutator> MutatorPtr;

}

#endif // HYPERTABLE_MUTATOR_H
