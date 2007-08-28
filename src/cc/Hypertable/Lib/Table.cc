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

#include <cstring>

#include "Common/Error.h"

#include "Table.h"

/**
 *
 */
Table::Table(InstanceDataPtr &instPtr, std::string &name) : mInstPtr(instPtr) {
  int error;
  std::string schemaStr;

  if ((error = mInstPtr->masterPtr->GetSchema(name.c_str(), schemaStr)) != Error::OK)
    throw Exception(error);

  mSchemaPtr.reset( Schema::NewInstance(schemaStr.c_str(), strlen(schemaStr.c_str()), true) );

  if (!mSchemaPtr->IsValid()) {
    LOG_VA_ERROR("Schema Parse Error: %s", mSchemaPtr->GetErrorString());
    throw Exception(Error::MASTER_BAD_SCHEMA);
  }

}


int Table::CreateMutator(MutatorPtr &mutatorPtr) {
  
}
