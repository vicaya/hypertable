/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package org.hypertable.Hypertable.RangeServer;

import java.io.File;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import org.hypertable.Common.Error;
import org.hypertable.Common.FileUtils;

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
