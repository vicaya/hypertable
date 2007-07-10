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
import java.nio.ByteBuffer;
import java.util.LinkedList;

import org.hypertable.Common.Error;
import org.hypertable.Common.FileUtils;

import org.hypertable.AsyncComm.CallbackHandlerSynchronizer;
import org.hypertable.AsyncComm.Event;

import org.hypertable.Hypertable.ParseCommandLine;
import org.hypertable.Hypertable.Schema;

class CommandUpdate extends Command {

    public CommandUpdate(String argsStr) throws IOException {
	super(argsStr);
    }

    public void run() {
	int msgId;
	ParseCommandLine.Argument arg;

	try {

	    if (mArgs.size() != 2) {
		DisplayUsage();
		return;
	    }

	    arg = mArgs.elementAt(0);
	    if (arg.value != null) {
		DisplayUsage();
		return;
	    }
	    RangeIdentifier range = new RangeIdentifier(arg.name);

	    arg = mArgs.elementAt(1);
	    if (arg.value != null) {
		DisplayUsage();
		return;
	    }
	    String dataFile = arg.name;
	    
	    String schemaFile = range.tableName + ".xml";

	    java.lang.System.out.println("Table Name  = " + range.tableName);
	    java.lang.System.out.println("Start Row   = " + range.startRow);
	    java.lang.System.out.println("End Row     = " + range.endRow);
	    java.lang.System.out.println("Schema File = " + schemaFile);
	    java.lang.System.out.println("Data File   = " + dataFile);

	    LinkedList<ByteBuffer>  modList = new LinkedList<ByteBuffer>();

	    byte [] bytes = FileUtils.FileToBuffer(new File(schemaFile));
	    String schemaStr = new String(bytes);

	    Schema schema = new Schema(schemaStr);

	    TestSource testSource = new TestSource(dataFile, schema);

	    ByteBuffer sendBuf = ByteBuffer.allocate(65536);
	    boolean outstanding = false;
	    Event event = null;

	    while (true) {

		int totalLen = 0;
		while ((bytes = testSource.Next()) != null) {
		    if (bytes.length <= sendBuf.remaining())
			sendBuf.put(bytes);
		    else
			break;
		}

		if (outstanding) {
		    event = mSyncHandler.WaitForEvent();
		    if (event == null)
			System.err.println("ERROR: Timed out waiting for response from Hypertable Master");
		    else if (Global.protocol.ResponseCode(event) == Error.RANGESERVER_PARTIAL_UPDATE) {
			System.out.println("partial update");
		    }
		    else if (Global.protocol.ResponseCode(event) != Error.OK) {
			System.err.println(Global.protocol.StringFormatMessage(event));
		    }
		    else
			System.out.println("success");
		}
		outstanding = false;

		if (sendBuf.position() > 0) {
		    byte [] mods = new byte [ sendBuf.position() ];
		    sendBuf.flip();
		    sendBuf.get(mods);

		    Global.client.Update(schema.GetGeneration(), range, mods, mSyncHandler);
		    outstanding = true;
		}

		if (bytes == null)
		    break;

		sendBuf.clear();
		sendBuf.put(bytes);
	    }

	    if (outstanding) {
		event = mSyncHandler.WaitForEvent();
		if (event == null)
		    System.err.println("ERROR: Timed out waiting for response from Hypertable Master");
		else if (Global.protocol.ResponseCode(event) == Error.RANGESERVER_PARTIAL_UPDATE) {
		    System.out.println("partial update");
		}
		else if (Global.protocol.ResponseCode(event) != Error.OK) {
		    System.err.println(Global.protocol.StringFormatMessage(event));
		}
		else
		    System.out.println("success");
	    }

	}
	catch (Throwable e) {
	    e.printStackTrace();
	}
    }

    private void DisplayUsage() {
	ReportError("usage: update <range> <dataFile>");
    }

}
