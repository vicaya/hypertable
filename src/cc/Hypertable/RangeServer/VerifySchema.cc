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

#include "Common/Error.h"

#include "Global.h"
#include "VerifySchema.h"

using namespace hypertable;

namespace hypertable {

  int VerifySchema(TableInfoPtr &tableInfoPtr, int generation, std::string &errMsg) {
    std::string tableFile = (std::string)"/hypertable/tables/" + tableInfoPtr->GetName();
    DynamicBuffer valueBuf(0);
    int error;
    SchemaPtr schemaPtr = tableInfoPtr->GetSchema();

    if (schemaPtr.get() == 0 || schemaPtr->GetGeneration() < generation) {

      if ((error = Global::hyperspace->AttrGet(tableFile.c_str(), "schema", valueBuf)) != Error::OK) {
	errMsg = (std::string)"Problem getting 'schema' attribute for '" + tableFile + "'";
	return error;
      }

      schemaPtr.reset( Schema::NewInstance((const char *)valueBuf.buf, valueBuf.fill(), true) );
      if (!schemaPtr->IsValid()) {
	errMsg = "Schema Parse Error for table '" + tableInfoPtr->GetName() + "' : " + schemaPtr->GetErrorString();
	return Error::RANGESERVER_SCHEMA_PARSE_ERROR;
      }

      tableInfoPtr->UpdateSchema(schemaPtr);

      // Generation check ...
      if ( schemaPtr->GetGeneration() < generation ) {
	errMsg = "Fetched Schema generation for table '" + tableInfoPtr->GetName() + "' is " + schemaPtr->GetGeneration() + " but supplied is " + generation;
	return Error::RANGESERVER_GENERATION_MISMATCH;
      }
    }
  
    return Error::OK;

  }
}
