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

package org.hypertable.Hypertable.RangeServer;

import java.io.File;
import java.io.IOException;
import java.io.StreamTokenizer;

import org.hypertable.Common.Error;
import org.hypertable.Common.FileUtils;

import org.hypertable.AsyncComm.CallbackHandlerSynchronizer;
import org.hypertable.AsyncComm.Event;

import org.hypertable.Hypertable.ParseCommandLine;
import org.hypertable.Hypertable.Schema;

class CommandCompact extends Command {

    public CommandCompact(String argsStr) throws IOException {
	super(argsStr);
    }

    public void run() {
	int msgId;
	boolean major = true;
	ParseCommandLine.Argument arg;

	try {

	    if (mArgs.size() < 1) {
		DisplayUsage();
		return;
	    }

	    arg = mArgs.elementAt(0);
	    if (arg.value != null) {
		DisplayUsage();
		return;
	    }
	    RangeIdentifier range = new RangeIdentifier(arg.name);

	    /**
	     * Load schema
	     */
	    String schemaFile = range.tableName + ".xml";
	    byte [] bytes = FileUtils.FileToBuffer(new File(schemaFile));
	    String schemaStr = new String(bytes);
	    Schema schema = new Schema(schemaStr);

	    String accessGroup = null;

	    for (int i=1; i<mArgs.size(); i++) {

		arg = mArgs.elementAt(i);

		if (arg.name.equals("ag")) {
		    accessGroup = arg.value;
		}
		else if (arg.name.equals("type")) {
		    if (arg.value != null) {
			if (arg.value.equals("minor")) {
			    major = false;
			}
			else if (!arg.value.equals("major")) {
			    DisplayUsage();
			    return;
			}
		    }
		}
		else {
		    DisplayUsage();
		    return;
		}
	    }

	    java.lang.System.out.println("Table Name        = " + range.tableName);
	    java.lang.System.out.println("Start Row         = " + range.startRow);
	    java.lang.System.out.println("End Row           = " + range.endRow);
	    java.lang.System.out.println("Access Group      = " + accessGroup);
	    java.lang.System.out.println("Major Compaction  = " + major);

	    Global.client.Compact(schema.GetGeneration(), range, major, accessGroup, mSyncHandler);

	    Event event = mSyncHandler.WaitForEvent();
	    
	    if (event == null)
		System.err.println("ERROR: Timed out waiting for response from Hypertable Master");
	    else if (Global.protocol.ResponseCode(event) != Error.OK) {
		System.err.println(Global.protocol.StringFormatMessage(event));
	    }
	    else
		System.out.println("success");
	}
	catch (Exception e) {
	    e.printStackTrace();
	}
    }

    private void DisplayUsage() {
	ReportError("usage:  compact <range> [ag=<accessGroup>] [type=major|minor]");
    }
}
