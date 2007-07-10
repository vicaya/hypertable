/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
