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



package org.hypertable.Hypertable;

import org.hypertable.Common.Error;

import org.hypertable.AsyncComm.MsgId;

public class TableMutator {

    public TableMutator() {
    }

    public int Set(String row, String columnFamily, String columnQualifier, long timestamp, byte [] value) {

	// Lookup tablet using row key
	
	// If location uknown, kick off a lookup and chain data onto a pending queue

	// If location known, add data to location specific queue

	return Error.OK;
    }


    int GroupCommit() {
	// For all of the 
	return Error.OK;
    }

    int WaitForResult() {
	//
	return Error.OK;
    }
    
}

