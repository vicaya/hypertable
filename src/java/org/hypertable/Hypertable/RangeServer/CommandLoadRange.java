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
import java.util.Vector;

import static java.lang.System.out;

import org.hypertable.Common.Error;
import org.hypertable.Common.FileUtils;

import org.hypertable.AsyncComm.Event;

import org.hypertable.Hypertable.ParseCommandLine;
import org.hypertable.Hypertable.Schema;

class CommandLoadRange extends Command {

    private String mCommandLine;

    public CommandLoadRange(String argsStr) throws IOException {
	super(argsStr);
    }

    public void run() {
	int msgId;
	ParseCommandLine.Argument arg;

	try {

	    if (mArgs.size() == 0) {
		DisplayUsage();
		return;
	    }

	    arg = mArgs.elementAt(0);

	    if (arg.value != null) {
		DisplayUsage();
		return;
	    }

	    RangeIdentifier range = new RangeIdentifier(arg.name);

	    java.lang.System.out.println("Table Name        = " + range.tableName);
	    java.lang.System.out.println("Start Row         = " + range.startRow);
	    java.lang.System.out.println("End Row           = " + range.endRow);

	    String schemaFile = range.tableName + ".xml";
	    java.lang.System.out.println("Schema File       = " + schemaFile);

	    byte [] schemaBytes = FileUtils.FileToBuffer(new File(schemaFile));

	    Schema schema = new Schema(new String(schemaBytes));
	    int tableGeneration = schema.GetGeneration();

	    java.lang.System.out.println("Schema Generation = " + tableGeneration);

	    Global.client.LoadRange(tableGeneration, range, mSyncHandler);

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
	ReportError("usage:  load range <range>");
    }
}
